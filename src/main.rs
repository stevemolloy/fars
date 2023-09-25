use crate::bpmdata::{BpmData, Ring};
use chrono::offset::TimeZone;
use chrono::prelude::*;
use chrono::Duration;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::env::args;
use std::io::{BufReader, Read, Result, Write};
use std::process::exit;
use threadpool::ThreadPool;

mod bpmdata;

static VERSION_NUMBER: &str = "1.0";

#[derive(Default)]
struct FastArchiverOptions {
    start_time: Option<DateTime<Local>>,
    end_time: Option<DateTime<Local>>,
    deci: bool,
    file: String,
    ring: Ring,
    find_dump: bool,
}

fn print_log_message(msg: &str) {
    println!("{}: {}", Local::now().timestamp_millis(), msg);
}

fn get_time_from_string(arg: String) -> Option<DateTime<Local>> {
    match Local.datetime_from_str(&arg, "%Y-%m-%dT%H:%M:%S%.f") {
        Ok(dt) => Some(dt),
        _ => None,
    }
}

fn print_error_and_exit(err: &str) {
    eprintln!("{}", err);
    exit(1);
}

fn root_mean_square(vec: &[i32]) -> f32 {
    let sum_squares = vec.iter().fold(0, |acc, &x| acc + (x as i64).pow(2));
    return ((sum_squares as f32) / (vec.len() as f32)).sqrt();
}

impl FastArchiverOptions {
    fn build_options(mut args_list: VecDeque<String>) -> Self {
        let mut opts: Self = Self::default();
        opts.file = "bpm".to_string();
        while !args_list.is_empty() {
            let next_arg = args_list.pop_front().unwrap();
            match next_arg.as_str() {
                "--start" => match args_list.pop_front() {
                    Some(expr) => opts.start_time = get_time_from_string(expr),
                    None => {
                        print_error_and_exit("Input parameters after `--start` are incorrect.");
                    }
                },
                "--end" => match args_list.pop_front() {
                    Some(expr) => opts.end_time = get_time_from_string(expr),
                    None => {
                        print_error_and_exit("Input parameters after `--end` are incorrect.");
                    }
                },
                "--ring" => match args_list.pop_front() {
                    Some(expr) => {
                        opts.ring = match expr.to_lowercase().as_str() {
                            "r1" => Ring::R1,
                            "r3" => Ring::R3,
                            _ => Ring::Unk,
                        }
                    }
                    None => {
                        print_error_and_exit("Input parameters after `--ring` are incorrect.");
                    }
                },
                "--file" => match args_list.pop_front() {
                    Some(expr) => opts.file = expr,
                    None => {
                        print_error_and_exit("Input parameters after `--file` are incorrect.");
                    }
                },
                "--deci" => opts.deci = true,
                "--find_dump" => opts.find_dump = true,
                _ => {}
            }
        }
        opts
    }

    fn check_options(&self) -> bool {
        let mut result: bool = true;
        if self.start_time.is_none() {
            eprintln!("No start time was given");
            result = false;
        }
        if self.end_time.is_none() {
            eprintln!("No end time was given");
            result = false;
        }
        if self.ring == Ring::Unk {
            eprintln!("No ring variable was given");
            result = false;
        }
        result
    }
}

fn get_fs(ring: Ring) -> Result<f64> {
    const HOST: &str = "fa";
    let port: u16 = match ring {
        Ring::R1 => 12001,
        Ring::R3 => 32001,
        Ring::Unk => {
            unreachable!("Should be impossible to get here...");
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
    ring: Ring,
    start_dt: &DateTime<Local>,
    end_dt: &DateTime<Local>,
    decimated: bool,
) -> Result<Vec<BpmData>> {
    print_log_message("Entered get_archived_data function");
    const HOST: &str = "fa";
    let port: u16;
    let bpm_range: Vec<String>;
    let bpm_cmd_str: String;

    let start_seconds = start_dt.timestamp();
    let start_nanos = start_dt.timestamp_nanos() - start_seconds * 1_000_000_000;
    let end_seconds = end_dt.timestamp();
    let end_nanos = end_dt.timestamp_nanos() - end_seconds * 1_000_000_000;

    match ring {
        Ring::R1 => {
            port = 12001;
            bpm_range = (1..37).map(|x| x.to_string()).collect();
            bpm_cmd_str = "1-36".to_string();
        }
        Ring::R3 => {
            port = 32001;
            bpm_range = (1..201).map(|x| x.to_string()).collect();
            bpm_cmd_str = "1-200".to_string();
        }
        Ring::Unk => unreachable!("Shouldn't be able to get here..."),
    };
    print_log_message(format!("Number of BPMs in data = {}", bpm_range.len()).as_str());
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
    print_log_message(format!("capacity_divisor: '{}'", capacity_divisor).as_str());

    let cmd_str = format!(
        "R{}M{}S{}.{:09}ES{}.{:09}N\n",
        acq_type, bpm_cmd_str, start_seconds, start_nanos, end_seconds, end_nanos,
    );
    print_log_message(format!("Sending the command: '{}'", cmd_str.trim()).as_str());

    let mut checkbyte = [0u8; CHKBYTESIZE];
    let mut header = [0u8; HDRSIZE];
    let total_time = (end_dt.timestamp_nanos() - start_dt.timestamp_nanos()) / 1_000_000_000;
    let mut buf = Vec::with_capacity((16222400 / capacity_divisor) * total_time as usize);

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd_str.as_bytes())?;
    print_log_message("Reading data from stream");
    stream.read_exact(&mut checkbyte)?;
    stream.read_exact(&mut header)?;
    let mut reader = BufReader::new(&stream);
    let read_bytes = reader.read_to_end(&mut buf)?;
    print_log_message(format!("Read {} bytes", read_bytes).as_str());

    print_log_message("Parsing data");
    let mut values = Vec::new();
    for i in (0..buf.len()).step_by(DATSIZE) {
        let val = i32::from_ne_bytes((&buf[i..i + DATSIZE]).try_into().unwrap());
        values.push(val);
    }
    let num_datapoints = values.len() / (2 * bpm_range.len());

    let fs = match get_fs(ring.clone()) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    };
    let timestep_nanoseconds: f64 = 1_000_000_000f64 / fs;

    let ts: Vec<_> = (1..num_datapoints)
        .map(|x| {
            (*start_dt
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
            ring: ring.clone(),
            bpmnum: i,
            ts: ts.clone(),
            x: x_vals,
            y: y_vals,
        };
        datasets.push(d);
    }

    print_log_message("Returning parsed data");
    Ok(datasets)
}

fn print_help(exe_name: &str) {
    println!("\nUsage:");
    print!("{exe_name} --ring R1|R3 ");
    print!("--start YYYY-MM-DDThh:mm:ss.xxx ");
    print!("--end YYYY-MM-DDThh:mm:ss.xxx ");
    println!("[-file basename]");
}

fn print_version(exe_name: &str) {
    println!("{exe_name} v{VERSION_NUMBER} (2023/09/23)");
}

fn main() {
    let mut args: VecDeque<String> = args().collect();
    let exe_name = args.pop_front().unwrap();
    if args.contains(&"--help".to_string()) || args.contains(&"-h".to_string()) {
        print_help(&exe_name);
        exit(0);
    }
    if args.contains(&"--version".to_string()) {
        print_version(&exe_name);
        exit(0);
    }

    let opts: FastArchiverOptions = FastArchiverOptions::build_options(args);

    if !opts.check_options() {
        eprintln!("Input parameters were not correct");
        print_help(&exe_name);
        exit(1);
    }

    let data;
    let initial_data;

    if opts.find_dump {
        initial_data = match get_archived_data(
            opts.ring.clone(),
            &opts.start_time.unwrap(),
            &opts.end_time.unwrap(),
            true,
        ) {
            Ok(reply) => reply,
            _ => {
                eprintln!("There was a problem getting data from the archiver.");
                eprintln!("Are you within the MAXIV firewall?");
                exit(1);
            }
        };
        print_log_message("Starting file-writing threads.");
        let pool = ThreadPool::new(7);
        for bpm in initial_data.clone() {
            pool.execute(move || {
                bpm.write_to_file("sparse_data");
            });
        }
        print_log_message("Waiting for file-write threads to finish.");
        pool.join();
        let data_length = initial_data[0].y.len();

        let dump_index: usize = (0..data_length - 1000)
            .map(|x| root_mean_square(&initial_data[0].y[x..(x + 1000)]))
            .position(|x| x > 1_000_000.0)
            .unwrap()
            + 1000;
        // println!("{:#?}", initial_data[0].ts[dump_index].clone());
        let dump_time: DateTime<Local> = Local
            .datetime_from_str(
                &initial_data[0].ts[dump_index].clone(),
                "%Y-%m-%d_%H:%M:%S%.f",
            )
            .unwrap();
        print_log_message(format!("Found a beam dump at {}", dump_time).as_str());

        let start_time = dump_time - Duration::milliseconds(4750);
        let end_time = dump_time + Duration::milliseconds(250);
        print_log_message(format!("Acquiring data from {} til {}", start_time, end_time).as_str());
        data = match get_archived_data(opts.ring, &start_time, &end_time, opts.deci) {
            Ok(reply) => reply,
            _ => {
                eprintln!("There was a problem getting data from the archiver.");
                eprintln!("Are you within the MAXIV firewall?");
                exit(1);
            }
        };
    } else {
        data = match get_archived_data(
            opts.ring,
            &opts.start_time.unwrap(),
            &opts.end_time.unwrap(),
            opts.deci,
        ) {
            Ok(reply) => reply,
            _ => {
                eprintln!("There was a problem getting data from the archiver.");
                eprintln!("Are you within the MAXIV firewall?");
                exit(1);
            }
        };
    }

    print_log_message("Starting file-writing threads.");
    let pool = ThreadPool::new(7);
    for bpm in data {
        let basename = opts.file.clone();
        pool.execute(move || {
            bpm.write_to_file(&basename);
        });
    }
    print_log_message("Waiting for file-write threads to finish.");
    pool.join();
    print_log_message("Done!");
}
