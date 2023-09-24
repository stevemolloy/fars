use chrono::offset::TimeZone;
use chrono::prelude::*;
use chrono::Duration;
use itertools::izip;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::env::args;
use std::fmt::Write as fmt_wrt;
use std::fs::File;
use std::io::BufReader;
use std::io::{Read, Result, Write};
use std::process::exit;
use threadpool::ThreadPool;

#[derive(Debug, Default)]
struct BpmData {
    ring: String,
    bpmnum: usize,
    ts: Vec<String>,
    x: Vec<i32>,
    y: Vec<i32>,
}

impl BpmData {
    fn output_string(self) -> String {
        let capacity = self.ts.len() * 100;
        let sum =
            izip!(self.ts, self.x, self.y).fold(String::with_capacity(capacity), |mut acc, x| {
                let _ = write!(acc, "{}, [{}, {}]\n", x.0, x.1, x.2);
                acc
            });
        return sum;
    }

    fn get_bpm_name(&self) -> String {
        let r3_bpmname_list = vec![
            "R3-301M1/DIA/BPM-01",
            "R3-301M1/DIA/BPM-02",
            "R3-301U1/DIA/BPM-01",
            "R3-301U2/DIA/BPM-01",
            "R3-301U3/DIA/BPM-01",
            "R3-301U3/DIA/BPM-02",
            "R3-301U4/DIA/BPM-01",
            "R3-301U5/DIA/BPM-01",
            "R3-301M2/DIA/BPM-01",
            "R3-301M2/DIA/BPM-02",
            "R3-302M1/DIA/BPM-01",
            "R3-302M1/DIA/BPM-02",
            "R3-302U1/DIA/BPM-01",
            "R3-302U2/DIA/BPM-01",
            "R3-302U3/DIA/BPM-01",
            "R3-302U3/DIA/BPM-02",
            "R3-302U4/DIA/BPM-01",
            "R3-302U5/DIA/BPM-01",
            "R3-302M2/DIA/BPM-01",
            "R3-302M2/DIA/BPM-02",
            "R3-303M1/DIA/BPM-01",
            "R3-303M1/DIA/BPM-02",
            "R3-303U1/DIA/BPM-01",
            "R3-303U2/DIA/BPM-01",
            "R3-303U3/DIA/BPM-01",
            "R3-303U3/DIA/BPM-02",
            "R3-303U4/DIA/BPM-01",
            "R3-303U5/DIA/BPM-01",
            "R3-303M2/DIA/BPM-01",
            "R3-303M2/DIA/BPM-02",
            "R3-304M1/DIA/BPM-01",
            "R3-304M1/DIA/BPM-02",
            "R3-304U1/DIA/BPM-01",
            "R3-304U2/DIA/BPM-01",
            "R3-304U3/DIA/BPM-01",
            "R3-304U3/DIA/BPM-02",
            "R3-304U4/DIA/BPM-01",
            "R3-304U5/DIA/BPM-01",
            "R3-304M2/DIA/BPM-01",
            "R3-304M2/DIA/BPM-02",
            "R3-305M1/DIA/BPM-01",
            "R3-305M1/DIA/BPM-02",
            "R3-305U1/DIA/BPM-01",
            "R3-305U2/DIA/BPM-01",
            "R3-305U3/DIA/BPM-01",
            "R3-305U3/DIA/BPM-02",
            "R3-305U4/DIA/BPM-01",
            "R3-305U5/DIA/BPM-01",
            "R3-305M2/DIA/BPM-01",
            "R3-305M2/DIA/BPM-02",
            "R3-306M1/DIA/BPM-01",
            "R3-306M1/DIA/BPM-02",
            "R3-306U1/DIA/BPM-01",
            "R3-306U2/DIA/BPM-01",
            "R3-306U3/DIA/BPM-01",
            "R3-306U3/DIA/BPM-02",
            "R3-306U4/DIA/BPM-01",
            "R3-306U5/DIA/BPM-01",
            "R3-306M2/DIA/BPM-01",
            "R3-306M2/DIA/BPM-02",
            "R3-307M1/DIA/BPM-01",
            "R3-307M1/DIA/BPM-02",
            "R3-307U1/DIA/BPM-01",
            "R3-307U2/DIA/BPM-01",
            "R3-307U3/DIA/BPM-01",
            "R3-307U3/DIA/BPM-02",
            "R3-307U4/DIA/BPM-01",
            "R3-307U5/DIA/BPM-01",
            "R3-307M2/DIA/BPM-01",
            "R3-307M2/DIA/BPM-02",
            "R3-308M1/DIA/BPM-01",
            "R3-308M1/DIA/BPM-02",
            "R3-308U1/DIA/BPM-01",
            "R3-308U2/DIA/BPM-01",
            "R3-308U3/DIA/BPM-01",
            "R3-308U3/DIA/BPM-02",
            "R3-308U4/DIA/BPM-01",
            "R3-308U5/DIA/BPM-01",
            "R3-308M2/DIA/BPM-01",
            "R3-308M2/DIA/BPM-02",
            "R3-309M1/DIA/BPM-01",
            "R3-309M1/DIA/BPM-02",
            "R3-309U1/DIA/BPM-01",
            "R3-309U2/DIA/BPM-01",
            "R3-309U3/DIA/BPM-01",
            "R3-309U3/DIA/BPM-02",
            "R3-309U4/DIA/BPM-01",
            "R3-309U5/DIA/BPM-01",
            "R3-309M2/DIA/BPM-01",
            "R3-309M2/DIA/BPM-02",
            "R3-310M1/DIA/BPM-01",
            "R3-310M1/DIA/BPM-02",
            "R3-310U1/DIA/BPM-01",
            "R3-310U2/DIA/BPM-01",
            "R3-310U3/DIA/BPM-01",
            "R3-310U3/DIA/BPM-02",
            "R3-310U4/DIA/BPM-01",
            "R3-310U5/DIA/BPM-01",
            "R3-310M2/DIA/BPM-01",
            "R3-310M2/DIA/BPM-02",
            "R3-311M1/DIA/BPM-01",
            "R3-311M1/DIA/BPM-02",
            "R3-311U1/DIA/BPM-01",
            "R3-311U2/DIA/BPM-01",
            "R3-311U3/DIA/BPM-01",
            "R3-311U3/DIA/BPM-02",
            "R3-311U4/DIA/BPM-01",
            "R3-311U5/DIA/BPM-01",
            "R3-311M2/DIA/BPM-01",
            "R3-311M2/DIA/BPM-02",
            "R3-312M1/DIA/BPM-01",
            "R3-312M1/DIA/BPM-02",
            "R3-312U1/DIA/BPM-01",
            "R3-312U2/DIA/BPM-01",
            "R3-312U3/DIA/BPM-01",
            "R3-312U3/DIA/BPM-02",
            "R3-312U4/DIA/BPM-01",
            "R3-312U5/DIA/BPM-01",
            "R3-312M2/DIA/BPM-01",
            "R3-312M2/DIA/BPM-02",
            "R3-313M1/DIA/BPM-01",
            "R3-313M1/DIA/BPM-02",
            "R3-313U1/DIA/BPM-01",
            "R3-313U2/DIA/BPM-01",
            "R3-313U3/DIA/BPM-01",
            "R3-313U3/DIA/BPM-02",
            "R3-313U4/DIA/BPM-01",
            "R3-313U5/DIA/BPM-01",
            "R3-313M2/DIA/BPM-01",
            "R3-313M2/DIA/BPM-02",
            "R3-314M1/DIA/BPM-01",
            "R3-314M1/DIA/BPM-02",
            "R3-314U1/DIA/BPM-01",
            "R3-314U2/DIA/BPM-01",
            "R3-314U3/DIA/BPM-01",
            "R3-314U3/DIA/BPM-02",
            "R3-314U4/DIA/BPM-01",
            "R3-314U5/DIA/BPM-01",
            "R3-314M2/DIA/BPM-01",
            "R3-314M2/DIA/BPM-02",
            "R3-315M1/DIA/BPM-01",
            "R3-315M1/DIA/BPM-02",
            "R3-315U1/DIA/BPM-01",
            "R3-315U2/DIA/BPM-01",
            "R3-315U3/DIA/BPM-01",
            "R3-315U3/DIA/BPM-02",
            "R3-315U4/DIA/BPM-01",
            "R3-315U5/DIA/BPM-01",
            "R3-315M2/DIA/BPM-01",
            "R3-315M2/DIA/BPM-02",
            "R3-316M1/DIA/BPM-01",
            "R3-316M1/DIA/BPM-02",
            "R3-316U1/DIA/BPM-01",
            "R3-316U2/DIA/BPM-01",
            "R3-316U3/DIA/BPM-01",
            "R3-316U3/DIA/BPM-02",
            "R3-316U4/DIA/BPM-01",
            "R3-316U5/DIA/BPM-01",
            "R3-316M2/DIA/BPM-01",
            "R3-316M2/DIA/BPM-02",
            "R3-317M1/DIA/BPM-01",
            "R3-317M1/DIA/BPM-02",
            "R3-317U1/DIA/BPM-01",
            "R3-317U2/DIA/BPM-01",
            "R3-317U3/DIA/BPM-01",
            "R3-317U3/DIA/BPM-02",
            "R3-317U4/DIA/BPM-01",
            "R3-317U5/DIA/BPM-01",
            "R3-317M2/DIA/BPM-01",
            "R3-317M2/DIA/BPM-02",
            "R3-318M1/DIA/BPM-01",
            "R3-318M1/DIA/BPM-02",
            "R3-318U1/DIA/BPM-01",
            "R3-318U2/DIA/BPM-01",
            "R3-318U3/DIA/BPM-01",
            "R3-318U3/DIA/BPM-02",
            "R3-318U4/DIA/BPM-01",
            "R3-318U5/DIA/BPM-01",
            "R3-318M2/DIA/BPM-01",
            "R3-318M2/DIA/BPM-02",
            "R3-319M1/DIA/BPM-01",
            "R3-319M1/DIA/BPM-02",
            "R3-319U1/DIA/BPM-01",
            "R3-319U2/DIA/BPM-01",
            "R3-319U3/DIA/BPM-01",
            "R3-319U3/DIA/BPM-02",
            "R3-319U4/DIA/BPM-01",
            "R3-319U5/DIA/BPM-01",
            "R3-319M2/DIA/BPM-01",
            "R3-319M2/DIA/BPM-02",
            "R3-320M1/DIA/BPM-01",
            "R3-320M1/DIA/BPM-02",
            "R3-320U1/DIA/BPM-01",
            "R3-320U2/DIA/BPM-01",
            "R3-320U3/DIA/BPM-01",
            "R3-320U3/DIA/BPM-02",
            "R3-320U4/DIA/BPM-01",
            "R3-320U5/DIA/BPM-01",
            "R3-320M2/DIA/BPM-01",
            "R3-320M2/DIA/BPM-02",
        ];
        let r1_bpmname_list = vec![
            "R1-101/DIA/BPM-01",
            "R1-101/DIA/BPM-02",
            "R1-101/DIA/BPM-03",
            "R1-102/DIA/BPM-01",
            "R1-102/DIA/BPM-02",
            "R1-102/DIA/BPM-03",
            "R1-103/DIA/BPM-01",
            "R1-103/DIA/BPM-02",
            "R1-103/DIA/BPM-03",
            "R1-104/DIA/BPM-01",
            "R1-104/DIA/BPM-02",
            "R1-104/DIA/BPM-03",
            "R1-105/DIA/BPM-01",
            "R1-105/DIA/BPM-02",
            "R1-105/DIA/BPM-03",
            "R1-106/DIA/BPM-01",
            "R1-106/DIA/BPM-02",
            "R1-106/DIA/BPM-03",
            "R1-107/DIA/BPM-01",
            "R1-107/DIA/BPM-02",
            "R1-107/DIA/BPM-03",
            "R1-108/DIA/BPM-01",
            "R1-108/DIA/BPM-02",
            "R1-108/DIA/BPM-03",
            "R1-109/DIA/BPM-01",
            "R1-109/DIA/BPM-02",
            "R1-109/DIA/BPM-03",
            "R1-110/DIA/BPM-01",
            "R1-110/DIA/BPM-02",
            "R1-110/DIA/BPM-03",
            "R1-111/DIA/BPM-01",
            "R1-111/DIA/BPM-02",
            "R1-111/DIA/BPM-03",
            "R1-112/DIA/BPM-01",
            "R1-112/DIA/BPM-02",
            "R1-112/DIA/BPM-03",
        ];

        if self.ring.to_lowercase() == "r3" {
            r3_bpmname_list[self.bpmnum].to_string()
        } else if self.ring.to_lowercase() == "r1" {
            r1_bpmname_list[self.bpmnum].to_string()
        } else {
            unreachable!("Shouldn't ever get here...");
        }
    }
}

fn get_fs(ring: &str) -> Result<f64> {
    const HOST: &str = "fa";
    let port: u16 = match ring.to_lowercase().as_str() {
        "r1" => 12001,
        "r3" => 32001,
        _ => {
            eprintln!("Could not understand the `--ring` flag; `{ring}`");
            exit(1);
        }
    };
    let cmd = "CF\n";

    let mut buf = Vec::new();

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd.as_bytes())?;
    stream.read_to_end(&mut buf)?;

    let info = std::str::from_utf8(&buf).unwrap();

    let infvec: Vec<&str> = info.split('\n').collect();
    let fs = infvec[0].parse::<f64>().unwrap();

    Ok(fs)
}

fn get_archived_data(
    ring: &str,
    start_string: &str,
    end_string: &str,
    decimated: bool,
) -> Result<Vec<BpmData>> {
    println!(
        "{}: entered get_archived_data function",
        Local::now().timestamp_millis()
    );
    const HOST: &str = "fa";
    let port: u16;
    let bpm_range: Vec<String>;
    let bpm_cmd_str: String;

    let start_dt = match Local.datetime_from_str(&start_string, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(dt) => dt,
        _ => {
            eprintln!("Could not understand the `--start` flag; `{start_string}`");
            exit(1);
        }
    };
    let end_dt = match Local.datetime_from_str(&end_string, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(dt) => dt,
        _ => {
            eprintln!("Could not understand the `--end` flag; `{end_string}`");
            exit(1);
        }
    };
    let start_seconds = start_dt.timestamp();
    let start_nanos = start_dt.timestamp_nanos() - start_seconds * 1_000_000_000;
    let end_seconds = end_dt.timestamp();
    let end_nanos = end_dt.timestamp_nanos() - end_seconds * 1_000_000_000;

    match ring.to_lowercase().as_str() {
        "r1" => {
            port = 12001;
            bpm_range = (1..37).map(|x| x.to_string()).collect();
            bpm_cmd_str = "1-36".to_string();
        }
        "r3" => {
            port = 32001;
            bpm_range = (1..201).map(|x| x.to_string()).collect();
            bpm_cmd_str = "1-200".to_string();
        }
        _ => panic!("Unknown ring: {}", ring),
    };
    println!(
        "{}: Number of BPMs in data = {}",
        Local::now().timestamp_millis(),
        bpm_range.len()
    );
    const CHKBYTESIZE: usize = 1;
    const HDRSIZE: usize = 8;
    const DATSIZE: usize = 4;

    let mut datasets = vec![];

    let acq_type: String = if decimated {
        "DF1".to_string()
    } else {
        "F".to_string()
    };
    let capacity_divisor = if decimated { 64 } else { 1 };
    println!(
        "{}: capacity_divisor: '{}'",
        Local::now().timestamp_millis(),
        capacity_divisor
    );

    let cmd_str = format!(
        "R{}M{}S{}.{:09}ES{}.{:09}N\n",
        acq_type, bpm_cmd_str, start_seconds, start_nanos, end_seconds, end_nanos,
    );
    println!(
        "{}: Sending the command: '{}'",
        Local::now().timestamp_millis(),
        cmd_str.trim()
    );

    let mut checkbyte = [0u8; CHKBYTESIZE];
    let mut header = [0u8; HDRSIZE];
    let total_time = (end_dt.timestamp_nanos() - start_dt.timestamp_nanos()) / 1_000_000_000;
    let mut buf = Vec::with_capacity((16222400 / capacity_divisor) * total_time as usize);

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd_str.as_bytes())?;
    println!(
        "{}: Reading data from stream",
        Local::now().timestamp_millis()
    );
    stream.read_exact(&mut checkbyte)?;
    stream.read_exact(&mut header)?;
    let mut reader = BufReader::new(&stream);
    let read_bytes = reader.read_to_end(&mut buf)?;
    println!(
        "{}: Read {} bytes",
        Local::now().timestamp_millis(),
        read_bytes
    );

    // println!("{:#?}", buf);
    println!("{}: Parsing data", Local::now().timestamp_millis());
    let mut values = Vec::new();
    for i in (0..buf.len()).step_by(DATSIZE) {
        let val = i32::from_ne_bytes((&buf[i..i + DATSIZE]).try_into().unwrap());
        values.push(val);
    }
    let num_datapoints = values.len() / (2 * bpm_range.len());

    let fs = match get_fs(&ring) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    };
    let timestep_nanoseconds: f64 = 1_000_000_000f64 / fs;

    let ts: Vec<_> = (1..num_datapoints)
        .map(|x| {
            (start_dt
                + Duration::nanoseconds(
                    ((x - 1) as f64 * timestep_nanoseconds * capacity_divisor as f64) as i64,
                ))
            .format("%Y-%m-%d_%H:%M:%S.%f")
            .to_string()
        })
        .collect();

    for (i, _) in bpm_range.iter().enumerate() {
        let x_vals = values[2 * i..]
            .iter()
            .step_by(bpm_range.len() * 2)
            .cloned()
            .collect::<Vec<i32>>();
        let y_vals = values[2 * i + 1..]
            .iter()
            .step_by(bpm_range.len() * 2)
            .cloned()
            .collect::<Vec<i32>>();
        let d = BpmData {
            ring: ring.to_string(),
            bpmnum: i,
            ts: ts.clone(),
            x: x_vals,
            y: y_vals,
        };
        datasets.push(d);
    }

    println!("{}: Returning parsed data", Local::now().timestamp_millis());
    Ok(datasets)
}

fn print_help(exe_name: &str) {
    println!("Acquires data from the Fast Archiver at MAXIV.\nUsage:");
    print!("{exe_name} --ring R1|R3 ");
    print!("--start YYYY-MM-DDThh:mm:ss.xxx ");
    print!("--end YYYY-MM-DDThh:mm:ss.xxx ");
    println!("[-file basename]");
}

fn print_version(exe_name: &str) {
    println!("{exe_name} v0.4 (2023/09/23)");
}

fn main() {
    let mut args: VecDeque<String> = args().collect();
    let exe_name = args.pop_front().unwrap();
    if args.contains(&"--help".to_string()) {
        print_help(&exe_name);
        exit(0);
    }
    if args.contains(&"--version".to_string()) {
        print_version(&exe_name);
        exit(0);
    }
    // if args.len() != 6 && args.len() != 8 {
    //     println!("\nCould not find the correct number of arguments.");
    //     print_help(&exe_name);
    //     exit(1);
    // }

    let mut start_time: String = "".to_string();
    let mut end_time: String = "".to_string();
    let mut ring: String = "".to_string();
    let mut filename: String = "bpm".to_string();
    let mut decimated: bool = false;

    while !args.is_empty() {
        let next_arg = args.pop_front().unwrap();
        match next_arg.as_str() {
            "-h" | "--help" => print_help(&exe_name),
            "--start" => match args.pop_front() {
                Some(expr) => start_time = expr,
                None => {
                    eprintln!("Input parameters were not correct");
                    print_help(&exe_name);
                }
            },
            "--end" => match args.pop_front() {
                Some(expr) => end_time = expr,
                None => {
                    eprintln!("Input parameters were not correct");
                    print_help(&exe_name);
                }
            },
            "--ring" => match args.pop_front() {
                Some(expr) => ring = expr,
                None => {
                    eprintln!("Input parameters were not correct");
                    print_help(&exe_name);
                }
            },
            "--file" => match args.pop_front() {
                Some(expr) => filename = expr,
                None => {
                    eprintln!("Input parameters were not correct");
                    print_help(&exe_name);
                }
            },
            "--deci" => decimated = true,
            e => {
                eprintln!("{e}");
                exit(1);
            }
        }
    }

    let data = match get_archived_data(&ring, &start_time, &end_time, decimated) {
        Ok(reply) => reply,
        _ => {
            eprintln!("There was a problem getting data from the archiver.");
            eprintln!("Are you within the MAXIV firewall?");
            exit(1);
        }
    };

    println!("{}: Writing to file.", Local::now().timestamp_millis());
    let pool = ThreadPool::new(7);
    for bpm in data {
        let basename = filename.clone();
        pool.execute(move || {
            write_bpmdata_to_file(&basename, bpm);
        });
    }
    println!(
        "{}: Waiting for file-write threads to finish. This can take some time for large datasets.",
        Local::now().timestamp_millis()
    );
    pool.join();
    println!("{}: Done!", Local::now().timestamp_millis());
}

fn write_bpmdata_to_file(basename: &str, bpm: BpmData) {
    let fname = format!("{}_{:03}.dat", basename, bpm.bpmnum);
    let mut file = File::create(fname).unwrap();
    write!(
        file,
        "\"# DATASET= tango://g-v-csdb-0.maxiv.lu.se:10000/{}/fa\"\n",
        bpm.get_bpm_name()
    )
    .unwrap();
    write!(file, "# t, [x, y]\n").unwrap();

    write!(file, "{}", bpm.output_string()).unwrap();
}
