use plotly::{Plot, Scatter};
use std::convert::TryInto;
use std::io::{Read, Result, Write};

#[derive(Debug)]
struct BpmData {
    x: Vec<i32>,
    y: Vec<i32>,
}

fn get_archived_data(ring: &str, bpms: &[u8], points: i32) -> Result<Vec<BpmData>> {
    const HOST: &str = "fa";
    let port: u16 = match ring.to_lowercase().as_str() {
        "r1" => 12001,
        "r3" => 32001,
        _ => panic!("Unknown ring: {}", ring),
    };
    const CHKBYTESIZE: usize = 1;
    const HDRSIZE: usize = 8;
    const DATSIZE: usize = 4;

    let mut datasets = vec![];

    let bpm_str = bpms
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let cmd = format!("RFM{bpm_str}S1693235869.000000000N{points}N\n");

    let mut checkbyte = [0u8; CHKBYTESIZE];
    let mut header = [0u8; HDRSIZE];
    let mut buf = Vec::new();

    let mut stream = std::net::TcpStream::connect((HOST, port))?;

    stream.write_all(cmd.as_bytes())?;
    stream.read_exact(&mut checkbyte)?;
    assert_eq!(checkbyte, [0]);
    stream.read_exact(&mut header)?;
    stream.read_to_end(&mut buf)?;

    let mut values = Vec::new();
    for i in (0..buf.len()).step_by(DATSIZE) {
        let val = i32::from_ne_bytes((&buf[i..i + DATSIZE]).try_into().unwrap());
        values.push(val);
    }

    for (i, _) in bpms.iter().enumerate() {
        let d = BpmData {
            x: values[2 * i..]
                .iter()
                .step_by(bpms.len() * 2)
                .cloned()
                .collect::<Vec<i32>>(),
            y: values[2 * i + 1..]
                .iter()
                .step_by(bpms.len() * 2)
                .cloned()
                .collect::<Vec<i32>>(),
        };
        datasets.push(d);
    }

    Ok(datasets)
}

fn main() {
    let bpms = [1, 2];
    let points = 200;

    let dataset = get_archived_data("R1", &bpms, points).unwrap();

    let mut plot = Plot::new();
    for ds in &dataset {
        let trace = Scatter::new(ds.x.clone(), ds.y.clone());
        plot.add_trace(trace);
    }

    plot.show();
}
