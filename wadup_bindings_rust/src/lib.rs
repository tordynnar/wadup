use std::io::{Read, Write, Seek};

#[macro_export]
macro_rules! wadup_start {
    ($name:ident) => {
        use wadup_bindings::wadup_error;

        #[unsafe(no_mangle)]
        pub extern "C" fn wadup_run() {
            if let Err(err) = $name() {
                let err = err.to_string();
                let err = err.as_bytes();
                unsafe {
                    wadup_error(err.as_ptr(), err.len());
                }
            }
        }
    }
}

pub struct WadupOutput {
    fd: i32,
    pos: u64,
}

impl WadupOutput {
    pub fn new() -> WadupOutput {
        let fd = unsafe { wadup_output_create() };
        WadupOutput {
            fd,
            pos: 0,
        }
    }

    fn len(&self) -> u64 {
        unsafe { wadup_output_len(self.fd) }
    }
}


impl Read for WadupOutput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buffer = buf.as_mut_ptr();
        let len = unsafe { wadup_output_read(self.fd, buffer, buf.len(), self.pos) };
        self.pos += len as u64;
        Ok(len)
    }
}

impl Write for WadupOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unsafe {
            wadup_output_write(self.fd, buf.as_ptr(), buf.len(), self.pos);
        }
        self.pos += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for WadupOutput {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            std::io::SeekFrom::Start(v) => {
                self.pos = v;
            },
            std::io::SeekFrom::End(v) => {
                let len = i64::try_from(self.len()).map_err(std::io::Error::other)?;
                self.pos = u64::try_from(len + v).map_err(std::io::Error::other)?;
            },
            std::io::SeekFrom::Current(v) => {
                let pos = i64::try_from(self.pos).map_err(std::io::Error::other)?;
                self.pos = u64::try_from(pos + v).map_err(std::io::Error::other)?;
            },
        }
        Ok(self.pos as u64)
    }
}

pub struct WadupInput {
    pos: u64,
    len: u64,
}

impl WadupInput {
    pub fn new() -> WadupInput {
        WadupInput {
            pos: 0,
            len: unsafe { wadup_input_len() },
        }
    }

    pub fn carve(length: u64, offset: u64) {
        unsafe { wadup_input_carve(length, offset) }
    }
}

impl Read for WadupInput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buffer = buf.as_mut_ptr();
        let len = unsafe { wadup_input_read(buffer, buf.len(), self.pos) };
        self.pos += len as u64;
        Ok(len)
    }
}

impl Seek for WadupInput {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            std::io::SeekFrom::Start(v) => {
                self.pos = v;
            },
            std::io::SeekFrom::End(v) => {
                let len = i64::try_from(self.len).map_err(std::io::Error::other)?;
                self.pos = u64::try_from(len + v).map_err(std::io::Error::other)?;
            },
            std::io::SeekFrom::Current(v) => {
                let pos = i64::try_from(self.pos).map_err(std::io::Error::other)?;
                self.pos = u64::try_from(pos + v).map_err(std::io::Error::other)?;
            },
        }
        Ok(self.pos as u64)
    }
}

pub struct WadupSchema {
    schema_index: u32,
}

impl WadupSchema {
    pub fn new(name: &str) -> WadupSchema {
        let name = name.as_bytes();
        WadupSchema {
            schema_index: unsafe { wadup_metadata_schema(name.as_ptr(), name.len()) }
        }
    }

    pub fn column_str(&self, name: &str) -> WadupColumnString {
        let name = name.as_bytes();
        WadupColumnString {
            schema_index: self.schema_index,
            column_index: unsafe { wadup_metadata_column(self.schema_index, name.as_ptr(), name.len(), COLUMN_STR) }
        }
    }

    pub fn column_i64(&self, name: &str) -> WadupColumnInt64 {
        let name = name.as_bytes();
        WadupColumnInt64 {
            schema_index: self.schema_index,
            column_index: unsafe { wadup_metadata_column(self.schema_index, name.as_ptr(), name.len(), COLUMN_I64) }
        }
    }

    pub fn column_f64(&self, name: &str) -> WadupColumnFloat64 {
        let name = name.as_bytes();
        WadupColumnFloat64 {
            schema_index: self.schema_index,
            column_index: unsafe { wadup_metadata_column(self.schema_index, name.as_ptr(), name.len(), COLUMN_F64) }
        }
    }

    pub fn flush_row(&self) {
        unsafe {
            wadup_metadata_flush_row(self.schema_index);
        }
    }
}

pub struct WadupColumnString {
    schema_index: u32,
    column_index: u32,
}

impl WadupColumnString {
    pub fn value(&self, value: &str) {
        let value = value.as_bytes();
        unsafe {
            wadup_metadata_value_str(self.schema_index, self.column_index, value.as_ptr(), value.len());
        }
    }
}

pub struct WadupColumnInt64 {
    schema_index: u32,
    column_index: u32,
}


impl WadupColumnInt64 {
    pub fn value(&self, value: i64) {
        unsafe {
            wadup_metadata_value_i64(self.schema_index, self.column_index, value);
        }
    }
}

pub struct WadupColumnFloat64 {
    schema_index: u32,
    column_index: u32,
}

impl WadupColumnFloat64 {
    pub fn value(&self, value: f64) {
        unsafe {
            wadup_metadata_value_f64(self.schema_index, self.column_index, value);
        }
    }
}

const COLUMN_STR: u32 = 1;
const COLUMN_I64: u32 = 2;
const COLUMN_F64: u32 = 3;

#[link(wasm_import_module = "host")]
unsafe extern "C" {
    fn wadup_input_read(buffer: *mut u8, length: usize, offset: u64) -> usize;
    fn wadup_input_len() -> u64;
    fn wadup_input_carve(length: u64, offset: u64);

    fn wadup_output_create() -> i32;
    fn wadup_output_read(fd: i32, buffer: *mut u8, length: usize, offset: u64) -> usize;
    fn wadup_output_write(fd: i32, buffer: *const u8, length: usize, offset: u64);
    fn wadup_output_len(fd: i32) -> u64;

    pub fn wadup_error(error: *const u8, error_length: usize);

    fn wadup_metadata_schema(schema_name: *const u8, schema_length: usize) -> u32;
    fn wadup_metadata_column(schema_index: u32, column_name: *const u8, column_length: usize, column_type: u32) -> u32;
    fn wadup_metadata_value_str(schema_index: u32, column_index: u32, value: *const u8, value_length: usize);
    fn wadup_metadata_value_i64(schema_index: u32, column_index: u32, value: i64);
    fn wadup_metadata_value_f64(schema_index: u32, column_index: u32, value: f64);
    fn wadup_metadata_flush_row(schema_index: u32);
}
