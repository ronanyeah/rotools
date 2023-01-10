use std::{
    io::{BufReader, Write},
    path::Path,
};

pub fn read<T: serde::de::DeserializeOwned, P: AsRef<Path>>(
    path: P,
) -> Result<T, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).map_err(|e| e.into())
}

pub fn write<T: serde::Serialize, P: AsRef<Path>>(
    path: P,
    data: T,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::File::create(path)?.write_all(serde_json::to_string(&data)?.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Scaf {
        id: i32,
    }

    #[test]
    fn test_create() {
        let path = format!(
            "/tmp/{}.json",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        write(&path, vec![1, 2, 3]).unwrap();
        let data: Vec<i32> = read(&path).unwrap();

        assert_eq!(data, [1, 2, 3]);
    }
}
