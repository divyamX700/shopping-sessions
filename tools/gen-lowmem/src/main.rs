//! `generate`, the Shopping Sessions dataset generator. The README's "Run it" and
//! "The generator" say what it writes and how to call it.

mod plan;
mod random;
mod text;
mod user;

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use plan::{CATEGORIES, CHANNELS, Dataset};
#[allow(unused_imports)]
use random::{Random, Stream};
use user::{Parts, Shard, generate_shard};

struct Arguments {
    seed: u64,
    events: u64,
    clicks: u64,
    output: PathBuf,
    threads: usize,
}

fn main() -> ExitCode {
    let Some(arguments) = parse_arguments() else {
        eprintln!(
            "usage: generate --seed N --events E --clicks C --output DIR [--threads T]   (E at least 1000)"
        );
        return ExitCode::from(2);
    };
    match run(&arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("generate: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse_arguments() -> Option<Arguments> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let value = |name: &str| {
        let at = arguments.iter().position(|argument| argument == name)?;
        arguments.get(at + 1).cloned()
    };
    let parsed = Arguments {
        seed: value("--seed")?.parse().ok()?,
        events: value("--events")?.parse().ok()?,
        clicks: value("--clicks")?.parse().ok()?,
        output: PathBuf::from(value("--output")?),
        threads: match value("--threads") {
            Some(threads) => threads.parse().ok().filter(|&threads| threads > 0)?,
            None => thread::available_parallelism().map_or(4, usize::from),
        },
    };
    (parsed.events >= 1_000).then_some(parsed)
}

fn run(arguments: &Arguments) -> io::Result<()> {
    let dataset = Dataset::plan(arguments.seed, arguments.events, arguments.clicks);
    let threads = arguments.threads;

    // Low-memory variant: generate shards in batches and append each batch's
    // rows to the part files. Every row and its file are the same as the
    // original generator's; only the line order inside a file differs.
    let output = &arguments.output;
    fs::create_dir_all(output.join("events"))?;
    fs::create_dir_all(output.join("clicks"))?;
    fs::create_dir_all(output.join("links"))?;
    let folders: [(&str, usize, Stream, &str); 3] = [
        ("events", dataset.event_parts.len(), Stream::ShuffleEvents, EVENT_HEADER),
        ("clicks", dataset.click_parts.len(), Stream::ShuffleClicks, CLICK_HEADER),
        ("links", dataset.link_parts.len(), Stream::ShuffleLinks, LINK_HEADER),
    ];
    for (folder, count, _, header) in folders {
        for part in 0..count {
            let path = output.join(folder).join(format!("part-{part:05}.csv"));
            fs::write(&path, format!("{header}
"))?;
        }
    }
    let batch = std::env::var("GEN_BATCH").ok().and_then(|v| v.parse().ok()).unwrap_or(32usize);
    let total = dataset.shards.len();
    let mut first = 0;
    while first < total {
        let last = (first + batch).min(total);
        let slots: Vec<Mutex<Option<Shard>>> = (first..last).map(|_| Mutex::new(None)).collect();
        let next = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| loop {
                    let at = next.fetch_add(1, Ordering::Relaxed);
                    let Some(slot) = slots.get(at) else { break };
                    let users = dataset.shards[first + at].clone();
                    *slot.lock().unwrap() = Some(generate_shard(&dataset, users));
                });
            }
        });
        let shards: Vec<Shard> = slots.into_iter().map(|s| s.into_inner().unwrap().unwrap()).collect();
        let event_parts: Vec<&Parts> = shards.iter().map(|shard| &shard.events).collect();
        let click_parts: Vec<&Parts> = shards.iter().map(|shard| &shard.clicks).collect();
        let link_parts: Vec<&Parts> = shards.iter().map(|shard| &shard.links).collect();
        let mut jobs: Vec<(&[&Parts], usize, &str, Stream)> = Vec::new();
        for (folder, count, stream, _) in folders {
            let parts = match folder {
                "events" => event_parts.as_slice(),
                "clicks" => click_parts.as_slice(),
                _ => link_parts.as_slice(),
            };
            for part in 0..count {
                jobs.push((parts, part, folder, stream));
            }
        }
        let next = AtomicUsize::new(0);
        let failure: Mutex<Option<io::Error>> = Mutex::new(None);
        thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| {
                    while let Some(&(parts, part, folder, stream)) =
                        jobs.get(next.fetch_add(1, Ordering::Relaxed))
                    {
                        let path = output.join(folder).join(format!("part-{part:05}.csv"));
                        let mut random = Random::new(dataset.seed ^ (first as u64) << 32, stream, part as u64);
                        if let Err(error) = append_part(&path, parts, part, &mut random) {
                            failure.lock().unwrap().get_or_insert(error);
                        }
                    }
                });
            }
        });
        if let Some(error) = failure.into_inner().unwrap() {
            return Err(error);
        }
        eprintln!("shards {last}/{total}");
        first = last;
    }

    // A new seed can write fewer files. Readers consume every CSV in each folder.
    for (folder, count) in [
        ("events", dataset.event_parts.len()),
        ("clicks", dataset.click_parts.len()),
        ("links", dataset.link_parts.len()),
    ] {
        remove_surplus_parts(&output.join(folder), count)?;
    }
    write_lookups(output, &dataset)
}

fn remove_surplus_parts(directory: &Path, count: usize) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let filename = entry.file_name();
        let Some(number) = filename
            .to_str()
            .and_then(|name| name.strip_prefix("part-"))
            .and_then(|name| name.strip_suffix(".csv"))
        else {
            continue;
        };
        if number.len() == 5
            && number.bytes().all(|byte| byte.is_ascii_digit())
            && number.parse::<usize>().is_ok_and(|part| part >= count)
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

const EVENT_HEADER: &str = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents";
const CLICK_HEADER: &str = "click_id,user_id,campaign_id,ts";
const LINK_HEADER: &str = "user_id_a,user_id_b";

fn append_part(path: &Path, shards: &[&Parts], part: usize, random: &mut Random) -> io::Result<()> {
    let mut lines: Vec<&[u8]> = Vec::new();
    for shard in shards {
        let bytes = &shard.bytes[part];
        let starts = &shard.starts[part];
        for (at, &start) in starts.iter().enumerate() {
            let end = starts.get(at + 1).map_or(bytes.len(), |&next| next as usize);
            lines.push(&bytes[start as usize..end]);
        }
    }
    if lines.is_empty() {
        return Ok(());
    }
    random.shuffle(&mut lines);
    let file = fs::OpenOptions::new().append(true).open(path)?;
    let mut writer = BufWriter::with_capacity(1 << 20, file);
    for line in lines {
        writer.write_all(line)?;
    }
    writer.flush()
}

/// One file: every shard's rows for this part, shuffled together, so no file
/// is sorted by time or user.
fn write_part(
    path: &Path,
    header: &str,
    shards: &[&Parts],
    part: usize,
    random: &mut Random,
) -> io::Result<()> {
    let mut lines: Vec<&[u8]> = Vec::new();
    for shard in shards {
        let bytes = &shard.bytes[part];
        let starts = &shard.starts[part];
        for (at, &start) in starts.iter().enumerate() {
            let end = starts
                .get(at + 1)
                .map_or(bytes.len(), |&next| next as usize);
            lines.push(&bytes[start as usize..end]);
        }
    }
    random.shuffle(&mut lines);
    let mut writer = BufWriter::with_capacity(1 << 20, File::create(path)?);
    writeln!(writer, "{header}")?;
    for line in lines {
        writer.write_all(line)?;
    }
    writer.flush()
}

fn write_lookups(out: &Path, dataset: &Dataset) -> io::Result<()> {
    let mut products = BufWriter::new(File::create(out.join("products.csv"))?);
    writeln!(products, "product_id,category")?;
    for (index, category) in dataset.product_category.iter().enumerate() {
        writeln!(
            products,
            "p{},{}",
            index + 1,
            CATEGORIES[*category as usize]
        )?;
    }
    products.flush()?;

    let mut campaigns = BufWriter::new(File::create(out.join("campaigns.csv"))?);
    writeln!(campaigns, "campaign_id,channel")?;
    for (index, channel) in dataset.campaign_channel.iter().enumerate() {
        writeln!(campaigns, "c_{},{}", index + 1, CHANNELS[*channel as usize])?;
    }
    campaigns.flush()?;

    let mut start = Vec::new();
    text::write_utc(&mut start, dataset.window_start);
    let mut end = Vec::new();
    text::write_utc(&mut end, dataset.window_end());
    fs::write(
        out.join("manifest.json"),
        format!(
            "{{\n  \"window_start\": \"{}\",\n  \"window_end\": \"{}\"\n}}\n",
            String::from_utf8_lossy(&start),
            String::from_utf8_lossy(&end)
        ),
    )
}
