use itertools::izip;
use regex::Regex;
use std::fmt::Write as fmt_wrt;
use std::fs::File;
use std::io::Write;

const R3_BPMNAME_LIST: &[&str] = &[
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

const R1_BPMNAME_LIST: &[&str] = &[
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

pub fn get_bpm_number(searchterms: &Vec<String>, ring: &Ring) -> Option<Vec<usize>> {
    let mut retval: Vec<usize> = vec![];

    for term in searchterms {
        match get_bpm_number_individual_term(term.to_uppercase().as_str(), ring) {
            Some(mut ans) => retval.append(&mut ans),
            None => {}
        }
    }

    if retval.is_empty() {
        None
    } else {
        Some(retval)
    }
}

fn get_bpm_number_individual_term(searchterm: &str, ring: &Ring) -> Option<Vec<usize>> {
    let re = match searchterm {
        "MIK" => Regex::new("^(R3-301M2/DIA/BPM-02|R3-302M1/DIA/BPM-01)$").unwrap(),
        "NANOMAX" => Regex::new("^(R3-302M2/DIA/BPM-02|R3-303M1/DIA/BPM-01)$").unwrap(),
        "DANMAX" => Regex::new("^(R3-303M2/DIA/BPM-02|R3-304M1/DIA/BPM-01)$").unwrap(),
        "BALDER" => Regex::new("^(R3-307M2/DIA/BPM-02|R3-308M1/DIA/BPM-01)$").unwrap(),
        "COSAXS" => Regex::new("^(R3-309M2/DIA/BPM-02|R3-310M1/DIA/BPM-01)$").unwrap(),
        "BIOMAX" => Regex::new("^(R3-310M2/DIA/BPM-02|R3-311M1/DIA/BPM-01)$").unwrap(),
        "VERITAS" => Regex::new("^(R3-315M2/DIA/BPM-02|R3-316M1/DIA/BPM-01)$").unwrap(),
        "HIPPIE" => Regex::new("^(R3-316M2/DIA/BPM-02|R3-317M1/DIA/BPM-01)$").unwrap(),
        "SOFTIMAX" => Regex::new("^(R3-317M2/DIA/BPM-02|R3-318M1/DIA/BPM-01)$").unwrap(),
        "FLEXPES" => Regex::new("^(R1-106/DIA/BPM-03|R1-107/DIA/BPM-01)$").unwrap(),
        "SPECIES" => Regex::new("^(R1-107/DIA/BPM-03|R1-108/DIA/BPM-01)$").unwrap(),
        "BLOCH" => Regex::new("^(R1-109/DIA/BPM-03|R1-110/DIA/BPM-01)$").unwrap(),
        "MAXPEEM" => Regex::new("^(R1-110/DIA/BPM-03|R1-111/DIA/BPM-01)$").unwrap(),
        "FINEST" => Regex::new("^(R1-111/DIA/BPM-03|R1-112/DIA/BPM-01)$").unwrap(),
        _ => Regex::new(format!("^{}$", searchterm).as_str()).unwrap(),
    };

    match *ring {
        Ring::R1 => Some(
            R1_BPMNAME_LIST
                .iter()
                .enumerate()
                .filter(|x| re.is_match(x.1))
                .map(|x| x.0 + 1)
                .collect(),
        ),
        Ring::R3 => Some(
            R3_BPMNAME_LIST
                .iter()
                .enumerate()
                .filter(|x| re.is_match(x.1))
                .map(|x| x.0 + 1)
                .collect(),
        ),
        Ring::Unk => None,
    }
}

pub fn get_bpm_name(bpmnum: usize, ring: &Ring) -> Option<String> {
    if *ring == Ring::R3 {
        Some(R3_BPMNAME_LIST[bpmnum].to_string())
    } else if *ring == Ring::R1 {
        Some(R1_BPMNAME_LIST[bpmnum].to_string())
    } else {
        None
    }
}

#[derive(PartialEq, Default, Debug, Clone)]
pub enum Ring {
    R1,
    R3,
    #[default]
    Unk,
}

#[derive(Debug, Default, Clone)]
pub struct BpmData {
    pub ring: Ring,
    pub bpmnum: usize,
    pub ts: Vec<String>,
    pub x: Vec<i32>,
    pub y: Vec<i32>,
}

impl BpmData {
    pub fn write_to_file(self, basename: &str) {
        let fname = format!("{}_{:03}.dat", basename, self.bpmnum);
        let mut file = File::create(fname).unwrap();
        writeln!(
            file,
            "\"# DATASET= tango://g-v-csdb-0.maxiv.lu.se:10000/{}/fa\"",
            get_bpm_name(self.bpmnum, &self.ring).unwrap()
        )
        .unwrap();
        writeln!(file, "# t [x, y]").unwrap();

        write!(file, "{}", self.output_string()).unwrap();
    }

    pub fn output_string(self) -> String {
        let capacity = self.ts.len() * 100;
        izip!(self.ts, self.x, self.y).fold(String::with_capacity(capacity), |mut acc, x| {
            let _ = writeln!(acc, "{} [{}, {}]", x.0, x.1, x.2);
            acc
        })
    }
}
