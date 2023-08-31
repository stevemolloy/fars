use chrono::offset::TimeZone;
use chrono::prelude::*;
use chrono::Duration;
use plotly::layout::Axis;
use plotly::layout::AxisType;
use plotly::Layout;
use plotly::Plot;
use plotly::Scatter;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::env::args;
use std::io::{Read, Result, Write};
use std::process::exit;

#[derive(Debug)]
struct BpmData {
    x: Vec<i32>,
    y: Vec<i32>,
}

fn get_archived_data(ring: &str, start_string: &str, end_string: &str) -> Result<Vec<BpmData>> {
    const HOST: &str = "fa";
    let port: u16;
    let bpm_range: Vec<String>;
    let bpm_cmd_str: String;

    println!("{start_string}, {end_string}");
    let start_dt = Local
        .datetime_from_str(start_string, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap();
    let end_dt = Local
        .datetime_from_str(end_string, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap();
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
    const CHKBYTESIZE: usize = 1;
    const HDRSIZE: usize = 8;
    const DATSIZE: usize = 4;

    let mut datasets = vec![];

    let cmd_str = format!(
        "RFM{}S{}.{:09}ES{}.{:09}N\n",
        bpm_cmd_str, start_seconds, start_nanos, end_seconds, end_nanos,
    );
    println!("Sending the command: '{cmd_str}'");

    let mut checkbyte = [0u8; CHKBYTESIZE];
    let mut header = [0u8; HDRSIZE];
    let mut buf = Vec::new();

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd_str.as_bytes())?;
    stream.read_exact(&mut checkbyte)?;
    // assert_eq!(checkbyte, [0]);
    stream.read_exact(&mut header)?;
    stream.read_to_end(&mut buf)?;

    let mut values = Vec::new();
    for i in (0..buf.len()).step_by(DATSIZE) {
        let val = i32::from_ne_bytes((&buf[i..i + DATSIZE]).try_into().unwrap());
        values.push(val);
    }

    for (i, _) in bpm_range.iter().enumerate() {
        let d = BpmData {
            x: values[2 * i..]
                .iter()
                .step_by(bpm_range.len() * 2)
                .cloned()
                .collect::<Vec<i32>>(),
            y: values[2 * i + 1..]
                .iter()
                .step_by(bpm_range.len() * 2)
                .cloned()
                .collect::<Vec<i32>>(),
        };
        datasets.push(d);
    }

    Ok(datasets)
}

fn print_help(exe_name: &str) {
    eprintln!("{exe_name} --ring R1|R3 --start YYYY-MM-DDThh:mm:ss.s --end YYYY-MM-DDThh:mm:ss.s");
}

fn main() {
    let mut args: VecDeque<String> = args().collect();
    let exe_name = args.pop_front().unwrap();
    if args.is_empty() {
        print_help(&exe_name);
        exit(1);
    }

    let mut start_time: String = "".to_string();
    let mut end_time: String = "".to_string();
    let mut ring: String = "".to_string();

    while !args.is_empty() {
        let next_arg = args.pop_front().unwrap();
        match next_arg.as_str() {
            "-h" | "--help" => print_help(&exe_name),
            "--start" => match args.pop_front() {
                Some(expr) => start_time = expr,
                None => todo!(),
            },
            "--end" => match args.pop_front() {
                Some(expr) => end_time = expr,
                None => todo!(),
            },
            "--ring" => match args.pop_front() {
                Some(expr) => ring = expr.to_lowercase(),
                None => todo!(),
            },
            e => {
                eprintln!("{e}");
                exit(1);
            }
        }
    }

    if start_time.is_empty() | end_time.is_empty() | ring.is_empty() {
        eprintln!("This must be called with all three arguments, as follows:");
        print_help(&exe_name);
        exit(1);
    }

    let dataset: Vec<BpmData>;
    match get_archived_data(&ring, &start_time, &end_time) {
        Ok(answer) => dataset = answer,
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    }

    let fs = 10073.698970;
    let timestep_nanoseconds: f64 = 1_000_000_000f64 / fs;
    let start_dt = Local
        .datetime_from_str(&start_time, "%Y-%m-%dT%H:%M:%S%.f")
        .unwrap();

    let mut plot = Plot::new();
    for ds in &dataset {
        let time_axis: Vec<_> = (1..ds.x.len()).collect();
        let trace = Scatter::new(
            time_axis
                .iter()
                .map(|x| {
                    start_dt + Duration::nanoseconds((*x as f64 * timestep_nanoseconds) as i64)
                })
                .collect(),
            ds.x.clone(),
        );
        plot.add_trace(trace);
    }
    let layout = Layout::new()
        .title("x Positions".into())
        .x_axis(Axis::new().type_(AxisType::Date));
    plot.set_layout(layout);
    plot.show();
}
