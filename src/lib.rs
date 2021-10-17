#[derive(serde::Deserialize, Debug)]
struct HasuraError {
    error: HasuraInfo,
}

#[derive(serde::Deserialize, Debug)]
struct HasuraInfo {
    description: Option<String>,
    exec_status: String,
    hint: Option<String>,
    message: String,
    status_code: String,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

/// # Errors
///
/// [GraphQL Spec](http://spec.graphql.org/draft/#sec-Errors)
pub async fn graphql_request<D: serde::de::DeserializeOwned>(
    req: reqwest::RequestBuilder,
) -> Result<D, Vec<String>> {
    let data = req.send().await.map_err(|e| vec![format!("{:?}", e)])?;

    let decode: reqwest::Result<graphql_client::Response<D>> = data.json().await;

    let body = decode.map_err(|e| vec![format!("{:?}", e)])?;

    match body.data {
        Some(d) => Ok(d),
        None => Err(match body.errors {
            Some(errors) => parse_errors(errors),
            None => vec!["hasura missing data".to_string()],
        }),
    }
}

/// # Errors
///
/// [GraphQL Spec](http://spec.graphql.org/draft/#sec-Errors)
pub async fn hasura_request<T: serde::Serialize, D: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    graphql_endpoint: &url::Url,
    hasura_admin_secret: &str,
    body: T,
) -> Result<D, Vec<String>> {
    let req = client
        .post(graphql_endpoint.as_str())
        .header("x-hasura-admin-secret", hasura_admin_secret.as_bytes())
        .json(&body);

    graphql_request(req).await
}

fn parse_errors(errors: Vec<graphql_client::Error>) -> Vec<String> {
    errors
        .into_iter()
        .map(|e| {
            let internal = e
                .extensions
                .as_ref()
                .and_then(|ext| ext.get("internal"))
                .and_then(|v| serde_json::from_value::<HasuraError>(v.clone()).ok())
                .map(|x| x.error);

            match internal {
                Some(h_err) => {
                    format!("{}\n{:?}", e.message, h_err)
                }
                None => e.message,
            }
        })
        .collect()
}
