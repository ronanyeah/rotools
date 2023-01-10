use persy::{ByteVec, Config, IndexType, Persy};

pub struct Dict<K, V> {
    db: persy::Persy,
    p1: std::marker::PhantomData<K>,
    p2: std::marker::PhantomData<V>,
}

const PRIMARY_INDEX: &str = "PRIMARY_INDEX";

impl<K, V> Dict<K, V>
where
    K: IndexType,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut url = std::path::PathBuf::new();
        url.push(path);

        if url.extension().map_or(true, |ext| ext != "persy") {
            return Err("Bad extension!")?;
        }

        let persy = Persy::open_or_create_with(&url, Config::new(), |db| {
            let mut tx = db.begin()?;
            tx.create_index::<K, ByteVec>(PRIMARY_INDEX, persy::ValueMode::Replace)?;
            tx.prepare()?.commit()?;
            Ok(())
        })?;

        Ok(Self {
            db: persy,
            p1: std::marker::PhantomData,
            p2: std::marker::PhantomData,
        })
    }
    pub fn insert(&self, k: K, v: &V) -> Result<(), Box<dyn std::error::Error>> {
        let mut tx = self.db.begin()?;
        let bts: ByteVec = ByteVec::new(serde_json::to_vec(&v)?);
        tx.put(PRIMARY_INDEX, k, bts)?;
        tx.prepare()?.commit()?;
        Ok(())
    }
    pub fn get(&self, k: &K) -> Result<Option<V>, Box<dyn std::error::Error>> {
        let pull = self.db.one::<_, ByteVec>(PRIMARY_INDEX, k)?;
        let res = if let Some(val) = &pull {
            let out = serde_json::from_slice(val)?;
            Some(out)
        } else {
            None
        };
        Ok(res)
    }
    pub fn member(&self, k: &K) -> Result<bool, Box<dyn std::error::Error>> {
        let pull = self.db.one::<_, ByteVec>(PRIMARY_INDEX, k)?;
        Ok(pull.is_some())
    }
    pub fn clear(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut tx = self.db.begin()?;
        tx.drop_index(PRIMARY_INDEX)?;
        tx.create_index::<K, ByteVec>(PRIMARY_INDEX, persy::ValueMode::Replace)?;
        tx.prepare()?.commit()?;
        Ok(())
    }
    pub fn size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self
            .db
            .range::<K, ByteVec, _>(PRIMARY_INDEX, ..)?
            .into_iter()
            .count())
    }
    pub fn keys(&self) -> Result<Vec<K>, Box<dyn std::error::Error>> {
        Ok(self
            .db
            .range::<_, ByteVec, _>(PRIMARY_INDEX, ..)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }
    pub fn values(&self) -> Result<Vec<V>, Box<dyn std::error::Error>> {
        self.db
            .range::<K, ByteVec, _>(PRIMARY_INDEX, ..)?
            .into_iter()
            .map(|(_, v)| {
                let data: Result<_, Box<dyn std::error::Error>> =
                    (|| Ok(serde_json::from_slice(&v.last().ok_or("missing value")?)?))();
                data
            })
            .collect()
    }
    pub fn pairs(&self) -> Result<Vec<(K, V)>, Box<dyn std::error::Error>> {
        self.db
            .range::<_, ByteVec, _>(PRIMARY_INDEX, ..)?
            .into_iter()
            .map(|(k, v)| {
                let data: Result<_, Box<dyn std::error::Error>> = (|| {
                    Ok((
                        k,
                        serde_json::from_slice(&v.last().ok_or("missing value")?)?,
                    ))
                })();
                data
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Scaf {
        id: i32,
    }

    fn get_path() -> String {
        format!(
            "/tmp/{}.persy",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    #[test]
    fn test_create() {
        let db = Dict::<i32, Scaf>::new(&get_path()).unwrap();
        assert_eq!(db.keys().unwrap().len(), 0);
        assert_eq!(db.values().unwrap().len(), 0);
        assert_eq!(db.pairs().unwrap().len(), 0);
    }

    #[test]
    fn test_fail() {
        assert!(Dict::<i32, Scaf>::new("").is_err());
        assert!(Dict::<i32, Scaf>::new("data.txt").is_err());
    }

    #[test]
    fn test_insert() {
        let db = Dict::<i32, Scaf>::new(&get_path()).unwrap();

        db.insert(0, &Scaf::default()).unwrap();
        assert_eq!(db.size().unwrap(), 1);

        db.insert(0, &Scaf::default()).unwrap();
        assert_eq!(db.size().unwrap(), 1);

        db.insert(1, &Scaf::default()).unwrap();
        assert_eq!(db.size().unwrap(), 2);
    }

    #[test]
    fn test_bulk() {
        let db = Dict::<i32, Scaf>::new(&get_path()).unwrap();

        db.insert(0, &Scaf::default()).unwrap();

        assert_eq!(db.keys().unwrap().len(), 1);
        assert_eq!(db.values().unwrap().len(), 1);
        assert_eq!(db.pairs().unwrap().len(), 1);

        db.clear().unwrap();

        assert_eq!(db.keys().unwrap().len(), 0);
        assert_eq!(db.values().unwrap().len(), 0);
        assert_eq!(db.pairs().unwrap().len(), 0);
    }
}
