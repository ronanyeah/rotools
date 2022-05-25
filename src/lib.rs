use anyhow::anyhow;
use serde::de::value::MapDeserializer;
use serde::Deserialize;
use std::fmt;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct HasuraInternalError {
    pub error: HasuraInternal,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct HasuraErrors {
    pub errors: Vec<ParsedError>,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct ParsedError {
    pub message: String,
    pub error: Option<HasuraInfo>,
}

// I think for postgres errors
#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct HasuraInternal {
    pub description: Option<String>,
    pub exec_status: String,
    pub hint: Option<String>,
    pub message: String,
    pub status_code: String,
}

// https://github.com/hasura/graphql-engine/blob/b6eb71ae07ed72965db51ed6a15af55f70730324/server/src-lib/Hasura/Base/Error.hs#L178
#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct HasuraInfo {
    pub code: String,
    pub path: String,
    pub error: Option<String>,
    pub internal: Option<HasuraInternalError>,
}

impl fmt::Display for HasuraErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string_pretty(&self.errors).map_err(|_| fmt::Error)?
        )
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

pub async fn graphql_request<D: serde::de::DeserializeOwned>(
    req: reqwest::RequestBuilder,
) -> anyhow::Result<graphql_client::Response<D>> {
    let res = req.send().await?;

    let res_ok = res.error_for_status()?;

    let decode: graphql_client::Response<D> = res_ok.json().await?;

    Ok(decode)
}

pub async fn graphql_parse<D: serde::de::DeserializeOwned>(
    body: graphql_client::Response<D>,
) -> anyhow::Result<D> {
    match body.data {
        Some(d) => Ok(d),
        None => match body.errors {
            Some(errors) => {
                let xs: Vec<_> = errors.into_iter().map(parse_error).collect();
                let ctx = HasuraErrors { errors: xs };
                Err(anyhow!(ctx))
            }
            None => Err(anyhow!("Hasura: No data or errors")),
        },
    }
}

pub async fn hasura_request<T: serde::Serialize, D: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    graphql_endpoint: &url::Url,
    hasura_admin_secret: &str,
    body: T,
) -> anyhow::Result<D> {
    let req = client
        .post(graphql_endpoint.as_str())
        .header("x-hasura-admin-secret", hasura_admin_secret.as_bytes())
        .json(&body);

    let res = graphql_request(req).await?;

    graphql_parse(res).await
}

pub fn parse_error(e: graphql_client::Error) -> ParsedError {
    let internal = e.extensions.as_ref().and_then(|ext| {
        HasuraInfo::deserialize(MapDeserializer::new(ext.clone().into_iter())).ok()
    });

    ParsedError {
        message: e.message,
        error: internal,
    }
}
