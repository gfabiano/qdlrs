// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) Qualcomm Technologies, Inc. and/or its subsidiaries.
use anstream::println;
use anyhow::Result;
use indexmap::IndexMap;
use owo_colors::OwoColorize;
use parsers::firehose_parser_ack_nak;
use serial::setup_serial_device;
use std::cmp::min;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::str::{self, FromStr};
use std::vec;
use types::FirehoseResetMode;
use types::FirehoseStatus;
use types::FirehoseStorageType;
use types::QdlBackend;
use types::QdlChan;
use types::QdlReadWrite;
use usb::setup_usb_device;

use android_sparse_image::{
    CHUNK_HEADER_BYTES_LEN, ChunkHeader, ChunkType, FileHeader, FileHeaderBytes
};

use anyhow::bail;
use pbr::{ProgressBar, Units};
use xmltree::{self, Element, XMLNode};

pub mod parsers;
pub mod sahara;
#[cfg(feature = "serial")]
pub mod serial;
pub mod types;
#[cfg(feature = "usb")]
pub mod usb;

pub fn setup_target_device(
    backend: QdlBackend,
    serial_no: Option<String>,
    port: Option<String>,
) -> Result<Box<dyn QdlReadWrite>> {
    match backend {
        QdlBackend::Serial => match setup_serial_device(port) {
            Ok(d) => Ok(Box::new(d)),
            Err(e) => Err(e),
        },
        QdlBackend::Usb => match setup_usb_device(serial_no) {
            Ok(d) => Ok(Box::new(d)),
            Err(e) => Err(e),
        },
    }
}

/// Wrapper for easily creating Firehose-y XML packets
fn firehose_xml_setup(op: &str, kvps: &[(&str, &str)]) -> anyhow::Result<Vec<u8>> {
    let mut xml = Element::new("data");
    let mut op_node = Element::new(op);
    for kvp in kvps.iter() {
        op_node
            .attributes
            .insert(kvp.0.to_owned(), kvp.1.to_owned());
    }

    xml.children.push(XMLNode::Element(op_node));

    // TODO: define a more verbose level
    // println!("SEND: {}", format!("{:?}", xml).bright_cyan());

    let mut buf = Vec::<u8>::new();
    xml.write(&mut buf)?;

    Ok(buf)
}

/// Main Firehose XML reading function
pub fn firehose_read<T: Read + Write + QdlChan>(
    channel: &mut T,
    response_parser: fn(&mut T, &IndexMap<String, String>) -> Result<FirehoseStatus, anyhow::Error>,
) -> Result<FirehoseStatus, anyhow::Error> {
    // xml_buffer_size comes from the device, so the XML should always fit
    let mut buf = vec![0u8; channel.fh_config().xml_buf_size];
    let mut got_any_data = false;

    loop {
        let bytes_read = match channel.read(&mut buf) {
            Ok(n) => n,
            Err(e) => match e.kind() {
                // In some cases (like with welcome messages), there's no acking
                // and a timeout is the "end of data" marker instead..
                std::io::ErrorKind::TimedOut => {
                    if got_any_data {
                        return Ok(FirehoseStatus::Ack);
                    } else {
                        return Err(e.into());
                    }
                }
                _ => return Err(e.into()),
            },
        };

        got_any_data = true;

        let xml_fragments_indices: Vec<_> = str::from_utf8(&buf[..bytes_read])?
            .match_indices("<?xml")
            .map(|s| s.0)
            .collect();

        for chunk in xml_fragments_indices.chunks(2) {
            let start = chunk[0];
            let end = *chunk.get(1).unwrap_or(&bytes_read);
            let xml = xmltree::Element::parse(&buf[start..end])?;

            if xml.name != "data" {
                // TODO: define a more verbose level
                if channel.fh_config().verbose_firehose {
                    println!("{:?}", xml);
                }
                bail!("Got a firehose packet without a data tag");
            }

            // The spec expects there's always a single node only
            if let Some(XMLNode::Element(e)) = xml.children.first() {
                // Check for a 'log' node and print out the message
                if e.name == "log" {
                    if channel.fh_config().skip_firehose_log {
                        continue;
                    }

                    println!(
                        "LOG: {}",
                        e.attributes
                            .get("value")
                            .to_owned()
                            .unwrap_or(&String::from("<garbage log data>"))
                            .bright_black()
                    );

                    continue;
                }

                // DEBUG: "print out incoming packets"
                // TODO: define a more verbose level
                if channel.fh_config().verbose_firehose {
                    println!("RECV: {}", format!("{e:?}").magenta());
                }

                // TODO: Use std::intrinsics::unlikely after it exits nightly
                if e.attributes.get("AttemptRetry").is_some() {
                    return firehose_read::<T>(channel, response_parser);
                } else if e.attributes.get("AttemptRestart").is_some() {
                    // TODO: handle this automagically
                    firehose_reset(channel, &FirehoseResetMode::ResetToEdl, 0)?;
                    bail!("Firehose requested a restart. Run the program again.");
                }

                // Pass other nodes to specialized parsers
                return response_parser(channel, &e.attributes);
            }
        }
    }
}

/// Send a Firehose packet
pub fn firehose_write<T: Read + Write + QdlChan>(
    channel: &mut T,
    buf: &mut [u8],
) -> anyhow::Result<()> {
    let mut b = buf.to_vec();

    // XML can't be n * 512 bytes long by fh spec
    if !buf.is_empty() && buf.len() % 512 == 0 {
        println!("{}", "INFO: Appending '\n' to outgoing XML".bright_black());
        b.push(b'\n');
    }

    match channel.write_all(&b) {
        Ok(_) => Ok(()),
        // Assume FH will hang after NAK..
        Err(_) => firehose_reset(channel, &FirehoseResetMode::ResetToEdl, 0),
    }
}

/// Send a Firehose packet and check for ack/nak
pub fn firehose_write_getack<T: Read + Write + QdlChan>(
    channel: &mut T,
    buf: &mut [u8],
    couldnt_what: String,
) -> anyhow::Result<()> {
    firehose_write(channel, buf)?;

    match firehose_read::<T>(channel, firehose_parser_ack_nak) {
        Ok(FirehoseStatus::Ack) => Ok(()),
        Ok(FirehoseStatus::Nak) => {
            // Assume FH will hang after NAK..
            firehose_reset(channel, &FirehoseResetMode::ResetToEdl, 0)?;
            Err(anyhow::Error::msg(format!("Couldn't {couldnt_what}")))
        }
        Err(e) => Err(e),
    }
}

/// Test performance without sample data
pub fn firehose_benchmark<T: Read + Write + QdlChan>(
    channel: &mut T,
    trials: u32,
    test_write_perf: bool,
) -> anyhow::Result<()> {
    let mut xml = firehose_xml_setup(
        "benchmark",
        &[
            ("trials", &trials.to_string()),
            (
                "TestWritePerformance",
                &(test_write_perf as u32).to_string(),
            ),
            (
                "TestReadPerformance",
                &(!test_write_perf as u32).to_string(),
            ),
        ],
    )?;

    firehose_write_getack(channel, &mut xml, "issue a NOP".to_owned())
}

/// Send a "Hello"-type packet to the Device
pub fn firehose_configure<T: Read + Write + QdlChan>(
    channel: &mut T,
    skip_storage_init: bool,
) -> anyhow::Result<()> {
    let config = channel.fh_config();
    // Spec requirement
    assert!(config.send_buffer_size % config.storage_sector_size == 0);
    // Sanity requirement
    assert!(config.send_buffer_size % config.storage_sector_size == 0);
    let mut xml = firehose_xml_setup(
        "configure",
        &[
            ("AckRawDataEveryNumPackets", "0"), // TODO: (low prio)
            (
                "SkipWrite",
                &(channel.fh_config().bypass_storage as u32).to_string(),
            ),
            ("SkipStorageInit", &(skip_storage_init as u32).to_string()),
            ("MemoryName", &config.storage_type.to_string()),
            ("AlwaysValidate", &(config.hash_packets as u32).to_string()),
            ("Verbose", &(config.verbose_firehose as u32).to_string()),
            ("MaxDigestTableSizeInBytes", "8192"), // TODO: (low prio)
            (
                "MaxPayloadSizeToTargetInBytes",
                &config.send_buffer_size.to_string(),
            ),
            // Zero-length-packet aware host
            ("ZLPAwareHost", "1"),
        ],
    )?;

    firehose_write(channel, &mut xml)
}

/// Do nothing, hopefully succesfully
pub fn firehose_nop<T: Read + Write + QdlChan>(channel: &mut T) -> anyhow::Result<()> {
    let mut xml = firehose_xml_setup("nop", &[("value", "ping")])?;

    firehose_write_getack(channel, &mut xml, "issue a NOP".to_owned())
}

/// Get information about the physical partition of a storage medium (e.g. LUN)
/// Prints to \<log\> only
pub fn firehose_get_storage_info<T: Read + Write + QdlChan>(
    channel: &mut T,
    phys_part_idx: u8,
) -> anyhow::Result<()> {
    let mut xml = firehose_xml_setup(
        "getstorageinfo",
        &[("physical_partition_number", &phys_part_idx.to_string())],
    )?;

    firehose_write(channel, &mut xml)?;

    firehose_read::<T>(channel, firehose_parser_ack_nak).and(Ok(()))
}

/// Alter Device (TODO: or Host) storage
pub fn firehose_patch<T: Read + Write + QdlChan>(
    channel: &mut T,
    byte_off: u64,
    phys_part_idx: u8,
    size: u64,
    start_sector: &str,
    val: &str,
) -> anyhow::Result<()> {
    let mut xml: Vec<u8> = firehose_xml_setup(
        "patch",
        &[
            (
                "SECTOR_SIZE_IN_BYTES",
                &channel.fh_config().storage_sector_size.to_string(),
            ),
            ("byte_offset", &byte_off.to_string()),
            ("filename", "DISK"), // DISK means "patch device's storage"
            ("physical_partition_number", &phys_part_idx.to_string()),
            ("size_in_bytes", &size.to_string()),
            ("start_sector", start_sector),
            ("value", val),
        ],
    )?;

    firehose_write_getack(channel, &mut xml, "patch".to_string())
}

/// Peek at memory
/// Prints to \<log\> only
pub fn firehose_peek<T: Read + Write + QdlChan>(
    channel: &mut T,
    addr: u64,
    byte_count: u64,
) -> anyhow::Result<()> {
    if channel.fh_config().skip_firehose_log {
        println!(
            "{}",
            "Warning: firehose <peek> only prints to <log>, remove --skip-firehose-log"
                .bright_red()
        );
    }

    let mut xml: Vec<u8> = firehose_xml_setup(
        "peek",
        &[
            ("address64", &addr.to_string()),
            ("size_in_bytes", &byte_count.to_string()),
        ],
    )?;

    firehose_write_getack(channel, &mut xml, format!("peek @ {addr:#x}"))
}

/// Poke at memory
/// This can lead to lock-ups and resets
// TODO:x
pub fn firehose_poke<T: Read + Write + QdlChan>(
    channel: &mut T,
    addr: u64,
    // TODO: byte count is 1..=8
    byte_count: u8,
    val: u64,
) -> anyhow::Result<()> {
    let mut xml: Vec<u8> = firehose_xml_setup(
        "poke",
        &[
            ("address64", &addr.to_string()),
            ("size_in_bytes", &byte_count.to_string()),
            ("value", &val.to_string()),
        ],
    )?;

    firehose_write_getack(channel, &mut xml, format!("peek @ {addr:#x}"))
}

/// Write to Device storage
pub fn firehose_program_storage<T: Read + Write + QdlChan>(
    channel: &mut T,
    data: &mut impl Read,
    label: &str,
    num_sectors: usize,
    phys_part_idx: u8,
    start_sector: &str,
) -> anyhow::Result<()> {
    let mut sectors_left = num_sectors;
    let mut xml = firehose_xml_setup(
        "program",
        &[
            (
                "SECTOR_SIZE_IN_BYTES",
                &channel.fh_config().storage_sector_size.to_string(),
            ),
            ("num_partition_sectors", &num_sectors.to_string()),
            ("physical_partition_number", &phys_part_idx.to_string()),
            ("start_sector", start_sector),
            (
                "read_back_verify",
                &(channel.fh_config().read_back_verify as u32).to_string(),
            ),
        ],
    )?;

    firehose_write(channel, &mut xml)?;

    if firehose_read::<T>(channel, firehose_parser_ack_nak)? != FirehoseStatus::Ack {
        bail!("<program> was NAKed. Did you set sector-size correctly?");
    }

    let mut pb = ProgressBar::new((sectors_left * channel.fh_config().storage_sector_size) as u64);
    pb.show_time_left = true;
    pb.message(&format!("Sending partition {label}: "));
    pb.set_units(Units::Bytes);

    while sectors_left > 0 {
        let chunk_size_sectors = min(
            sectors_left,
            channel.fh_config().send_buffer_size / channel.fh_config().storage_sector_size,
        );
        let mut buf = vec![
            0u8;
            min(
                channel.fh_config().send_buffer_size,
                chunk_size_sectors * channel.fh_config().storage_sector_size,
            )
        ];
        let _ = data.read(&mut buf).unwrap();

        let n = channel.write(&buf).expect("Error sending data");
        if n != chunk_size_sectors * channel.fh_config().storage_sector_size {
            bail!("Wrote an unexpected number of bytes ({})", n);
        }

        sectors_left -= chunk_size_sectors;
        pb.add((chunk_size_sectors * channel.fh_config().storage_sector_size) as u64);
    }

    // Send a Zero-Length Packet to indicate end of stream
    if channel.fh_config().backend == QdlBackend::Usb && !channel.fh_config().skip_usb_zlp {
        let _ = channel.write(&[]).expect("Error sending ZLP");
    }

    if firehose_read::<T>(channel, firehose_parser_ack_nak)? != FirehoseStatus::Ack {
        bail!("Failed to complete 'write' op");
    }

    Ok(())
}

/// Write sparse image to Device storage
pub fn firehose_program_storage_sparse<T: Read + Write + QdlChan, Q: Read + Seek>(
    channel: &mut T,
    data: &mut Q,
    label: &str,
    num_sectors: usize,
    phys_part_idx: u8,
    start_sector: &str,
    data_sector_offset: u32,
) -> anyhow::Result<()> {
    struct FlashGroup {
        start_sector: usize,
        data_offset: u64,
        expanded_size: usize,
        chunks: Vec<ChunkHeader>,
    }

    let mut flash_groups = vec![];

    let mut header_bytes = FileHeaderBytes::default();
    data.read_exact(&mut header_bytes)?;
    let header = FileHeader::from_bytes(&header_bytes);
    match header {
        Ok(ref header) => {
            println!("Preparing to flash android sparse image");
            let mut flash_group_chunks = vec![];
            let mut flash_group_size: usize = 0;
            let mut flash_group_offset: u64 = 0;
            let mut flash_group_start_sector = start_sector.parse::<usize>().unwrap();

            for _ in 0..header.chunks {
                let mut chunk_bytes = [0; CHUNK_HEADER_BYTES_LEN];
                data.read_exact(&mut chunk_bytes)?;
                let chunk = ChunkHeader::from_bytes(&chunk_bytes)?;
                let data_size = chunk.data_size();
                let out_size = chunk.out_size(header);

                if chunk.chunk_type == ChunkType::Raw || chunk.chunk_type == ChunkType::Fill {
                    if flash_group_chunks.is_empty() {
                        // means is starting a new flash group
                        flash_group_size = out_size;
                        flash_group_offset = data.stream_position()?;
                    } else {
                        flash_group_size += out_size;
                    }
                    flash_group_chunks.push(chunk);
                } else if !flash_group_chunks.is_empty() {
                    let current_flash_group = FlashGroup {
                        start_sector: flash_group_start_sector,
                        data_offset: flash_group_offset,
                        expanded_size: flash_group_size,
                        chunks: flash_group_chunks,
                    };
                    flash_groups.push(current_flash_group);

                    // reinitialize next flash group
                    flash_group_chunks = vec![];
                    flash_group_size = 0;
                    flash_group_offset = 0;
                }

                flash_group_start_sector += out_size / channel.fh_config().storage_sector_size;

                data.seek(SeekFrom::Current(data_size as i64))?;
            }
        }
        Err(android_sparse_image::ParseError::UnknownMagic) => {
            // Sparse attribute shall be wrong, fallback to raw programming
            data.seek(SeekFrom::Start(
                channel.fh_config().storage_sector_size as u64 * data_sector_offset as u64,
            ))?;

            firehose_program_storage(
                channel,
                data,
                label,
                num_sectors,
                phys_part_idx,
                start_sector,
            )?;
            return Ok(());
        }
        Err(e) => {
            bail!("Unable to parse sparse image: {e}")
        }
    };

    let block_size = header?.block_size;
    for flash_group in flash_groups {
        let mut sectors_left = flash_group.expanded_size / channel.fh_config().storage_sector_size;
        let mut xml = firehose_xml_setup(
            "getsha256digest",
            &[
                (
                    "SECTOR_SIZE_IN_BYTES",
                    &channel.fh_config().storage_sector_size.to_string(),
                ),
                ("num_partition_sectors", &sectors_left.to_string()),
                ("physical_partition_number", &phys_part_idx.to_string()),
                ("start_sector", &flash_group.start_sector.to_string()),
            ],
        )?;

        firehose_write(channel, &mut xml)?;

        if firehose_read::<T>(channel, firehose_parser_ack_nak)? != FirehoseStatus::Ack {
            bail!("<program> was NAKed. Did you set sector-size correctly?");
        }

        let mut pb =
            ProgressBar::new((sectors_left * channel.fh_config().storage_sector_size) as u64);
        pb.show_time_left = true;
        pb.message(&format!("Sending partition {label}: "));
        pb.set_units(Units::Bytes);

        // Go to the start of chunks
        let _ = data.seek(SeekFrom::Start(flash_group.data_offset));

        for chunk in flash_group.chunks {
            let out_size = chunk.chunk_size as usize * block_size as usize; // bytes
            let chunk_size_sectors = min(
                min(
                    sectors_left,
                    channel.fh_config().send_buffer_size / channel.fh_config().storage_sector_size,
                ),
                out_size / channel.fh_config().storage_sector_size,
            );
            let mut buf = vec![
                0u8;
                min(
                    channel.fh_config().send_buffer_size,
                    chunk_size_sectors * channel.fh_config().storage_sector_size,
                )
            ];
            if chunk.chunk_type == ChunkType::Raw {
                let _ = data.read(&mut buf).unwrap();
            } else {
                let mut fill_value = vec![0u8; 4];
                let _ = data.read(&mut fill_value);

                for _ in 0..out_size / 4 {
                    let _ = buf.write(&fill_value[..]);
                }
            }

            let n = channel.write(&buf).expect("Error sending data");
            if n != chunk_size_sectors * channel.fh_config().storage_sector_size {
                bail!("Wrote an unexpected number of bytes ({})", n);
            }

            sectors_left -= chunk_size_sectors;
            pb.add((chunk_size_sectors * channel.fh_config().storage_sector_size) as u64);
        }
    }

    Ok(())
}

/// Get a SHA256 digest of a portion of Device storage
pub fn firehose_checksum_storage<T: Read + Write + QdlChan>(
    channel: &mut T,
    num_sectors: usize,
    phys_part_idx: u8,
    start_sector: u32,
) -> anyhow::Result<()> {
    let mut xml = firehose_xml_setup(
        "getsha256digest",
        &[
            (
                "SECTOR_SIZE_IN_BYTES",
                &channel.fh_config().storage_sector_size.to_string(),
            ),
            ("num_partition_sectors", &num_sectors.to_string()),
            ("physical_partition_number", &phys_part_idx.to_string()),
            ("start_sector", &start_sector.to_string()),
        ],
    )?;

    firehose_write(channel, &mut xml)?;

    // TODO: figure out some sane way to figure out the timeout
    if firehose_read::<T>(channel, firehose_parser_ack_nak)? != FirehoseStatus::Ack {
        bail!("Checksum request was NAKed");
    }

    Ok(())
}

/// Read (sector-aligned) parts of storage.
pub fn firehose_read_storage<T: Read + Write + QdlChan>(
    channel: &mut T,
    out: &mut impl Write,
    num_sectors: usize,
    phys_part_idx: u8,
    start_sector: u32,
) -> anyhow::Result<()> {
    let mut sectors_left = num_sectors;
    let mut xml = firehose_xml_setup(
        "read",
        &[
            (
                "SECTOR_SIZE_IN_BYTES",
                &channel.fh_config().storage_sector_size.to_string(),
            ),
            ("num_partition_sectors", &num_sectors.to_string()),
            ("physical_partition_number", &phys_part_idx.to_string()),
            ("start_sector", &start_sector.to_string()),
        ],
    )?;

    firehose_write(channel, &mut xml)?;
    if firehose_read(channel, firehose_parser_ack_nak)? != FirehoseStatus::Ack {
        bail!("Read request was NAKed");
    }

    let mut pb = ProgressBar::new((sectors_left * channel.fh_config().storage_sector_size) as u64);
    pb.set_units(Units::Bytes);

    let mut last_read_was_zero_len = false;
    while sectors_left > 0 {
        let chunk_size_sectors = min(
            sectors_left,
            channel.fh_config().recv_buffer_size / channel.fh_config().storage_sector_size,
        );
        let mut buf = vec![
            0;
            min(
                channel.fh_config().recv_buffer_size,
                chunk_size_sectors * channel.fh_config().storage_sector_size
            )
        ];

        let n = channel.read(&mut buf).expect("Error receiving data");
        if n == 0 {
            // TODO: need more robustness here
            /* Every 2 or 3 packets should be empty? */
            last_read_was_zero_len = true;
            continue;
        } else if n != chunk_size_sectors * channel.fh_config().storage_sector_size {
            bail!("Read an unexpected number of bytes ({})", n);
        }

        last_read_was_zero_len = false;
        let _ = out.write(&buf)?;

        sectors_left -= chunk_size_sectors;
        pb.add((chunk_size_sectors * channel.fh_config().storage_sector_size) as u64);
    }

    if !last_read_was_zero_len && channel.fh_config().backend == QdlBackend::Usb {
        // Issue a dummy read to drain the queue
        let _ = channel.read(&mut [])?;
    }

    if firehose_read::<T>(channel, firehose_parser_ack_nak)? != FirehoseStatus::Ack {
        bail!("Failed to complete 'read' op");
    }

    Ok(())
}

/// Reboot or power off the Device
pub fn firehose_reset<T: Read + Write + QdlChan>(
    channel: &mut T,
    mode: &FirehoseResetMode,
    delay_in_sec: u32,
) -> anyhow::Result<()> {
    let mut xml = firehose_xml_setup(
        "power",
        &[
            (
                "value",
                match mode {
                    FirehoseResetMode::ResetToEdl => "reset_to_edl",
                    FirehoseResetMode::Reset => "reset",
                    FirehoseResetMode::Off => "off",
                },
            ),
            ("DelayInSeconds", &delay_in_sec.to_string()),
        ],
    )?;

    firehose_write_getack(channel, &mut xml, "reset the Device".to_owned())
}

/// Mark a physical storage partition as bootable
pub fn firehose_set_bootable<T: Read + Write + QdlChan>(
    channel: &mut T,
    drive_idx: u8,
) -> anyhow::Result<()> {
    let mut xml = firehose_xml_setup(
        "setbootablestoragedrive",
        &[("value", &drive_idx.to_string())],
    )?;

    firehose_write_getack(
        channel,
        &mut xml,
        format!("set partition {drive_idx} as bootable"),
    )
}

pub fn firehose_get_default_sector_size(t: &str) -> Option<usize> {
    match FirehoseStorageType::from_str(t).unwrap() {
        FirehoseStorageType::Emmc => Some(512),
        FirehoseStorageType::Nand => Some(4096),
        FirehoseStorageType::Nvme => Some(512),
        FirehoseStorageType::Ufs => Some(4096),
        FirehoseStorageType::Spinor => Some(4096),
    }
}
