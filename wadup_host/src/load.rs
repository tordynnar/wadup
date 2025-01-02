use std::io::{Read, Write};
use anyhow::{Result, anyhow};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use wasmtime::{Engine, Module};
use std::time::UNIX_EPOCH;

pub fn read_u64_le<R: Read>(input: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    input.read(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

pub fn write_u64_le<W: Write>(output: &mut W, value: u64) -> Result<()> {
    output.write_all(value.to_le_bytes().as_ref())?;
    Ok(())
}

pub fn read_compiled(module_compiled_path: &Path, engine_hash: u64, module_modified: u64) -> Result<Vec<u8>> {
    if !module_compiled_path.exists() {
        return Err(anyhow!("Compiled module path doesn't exist"))
    }

    let mut file = File::open(module_compiled_path)?;

    if read_u64_le(&mut file)? != engine_hash {
        return Err(anyhow!("Engine hash doesn't match"))
    };

    if read_u64_le(&mut file)? != module_modified {
        return Err(anyhow!("Module modified doesn't match"))
    };

    let mut result = Vec::new();
    file.read_to_end(&mut result)?;

    Ok(result)
}

pub fn load_module(engine: &Engine, module_path: &PathBuf) -> Result<(String, Module)> {
    let mut hasher = DefaultHasher::new();
    engine.precompile_compatibility_hash().hash(&mut hasher);
    let engine_hash = hasher.finish();

    let module_modified = fs::metadata(module_path)?.modified()?.duration_since(UNIX_EPOCH)?.as_secs();

    let module_compiled_path = format!("{}_precompiled", module_path.display());
    let module_compiled_path = Path::new(&module_compiled_path);

    let module = if let Some(module_compiled) = read_compiled(module_compiled_path, engine_hash, module_modified).ok() {
        unsafe { Module::deserialize(&engine, &module_compiled) }?
    } else {
        let module = Module::from_file(&engine, module_path)?;
        let mut file = File::create(module_compiled_path)?;
        write_u64_le(&mut file, engine_hash)?;
        write_u64_le(&mut file, module_modified)?;
        let module_serialized = module.serialize()?;
        file.write_all(module_serialized.as_slice())?;
        module
    };

    let name = module_path
        .file_name().ok_or_else(|| anyhow!("unable to get module file name"))?
        .to_str().ok_or_else(|| anyhow!("unable to convert module file name to string"))?
        .to_owned();

    Ok((name, module))
}