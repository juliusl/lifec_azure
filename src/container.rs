use azure_core::{
    auth::TokenCredential,
};
use azure_identity::DefaultAzureCredential;
use azure_storage::core::prelude::*;
use azure_storage_blobs::prelude::*;
use futures::StreamExt;
use lifec::{
    plugins::{Plugin, ThunkContext},
    Component, DenseVecStorage, AttributeGraph,
};

/// Plugin provides access to azure container
///
#[derive(Clone, Default, Component)]
#[storage(DenseVecStorage)]
pub struct Container;

impl Container {
    /// Resolves a blob client for the given blob/container
    /// 
    /// currently supports use_default_credentials
    pub async fn resolve_blob_client(tc: impl AsRef<AttributeGraph>, container_name: impl AsRef<str>, blob_name: impl AsRef<str>) -> Option<BlobClient> {
        let container_name = container_name.as_ref().to_string();
        let blob_name = blob_name.as_ref().to_string();

        if let Some(account_name) = tc.as_ref().find_text("account_name") {
            if tc
                .as_ref()
                .is_enabled("use_default_credentials")
                .unwrap_or_default()
            {
                match DefaultAzureCredential::default()
                    .get_token("https://storage.azure.com/")
                    .await
                {
                    Ok(bearer_token) => {
                        let blob_client =
                        StorageClient::new_bearer_token(&account_name, bearer_token.token.secret())
                            .container_client(container_name)
                            .blob_client(blob_name);

                        return Some(blob_client);
                    },
                    Err(err) => {
                        eprintln!("error getting token {err}")
                    },
                }
            }
        }

        None 
    }
}

impl Plugin<ThunkContext> for Container {
    fn symbol() -> &'static str {
        "container"
    }

    fn description() -> &'static str {
        "Fetches content for a blob from a container w/ `container_name` and blob w/ `blob_name`"
    }

    fn caveats() -> &'static str {
        "Uses the environment variables STORAGE_ACCOUNT/STORAGE_ACCESS_KEY to authenticate"
    }

    fn call_with_context(context: &mut ThunkContext) -> Option<lifec::plugins::AsyncContext> {
        context.clone().task(|_| {
            let mut tc = context.clone();
            async move {
                if let Some(container_name) = tc.as_ref().find_text("container_name") {
                    if let Some(blob_name) = tc.as_ref().find_text("blob_name") {
                        if let Some(blob_client) = Self::resolve_blob_client(&tc, container_name, blob_name).await {
                            let mut complete_response = vec![];
                            // this is how you stream a blob. You can specify the range(...) value as above if necessary.
                            // In this case we are retrieving the whole blob in 8KB chunks.
                            let mut stream = blob_client.get().chunk_size(0x2000u64).into_stream();
                            while let Some(value) = stream.next().await {
                                match value {
                                    Ok(data) => {
                                        complete_response.extend(&data.data);
    
                                        tc.as_mut().add_binary_attr("content", complete_response);
    
                                        return Some(tc);
                                    }
                                    Err(err) => {
                                        eprintln!("error getting content from blob {err}");
                                    }
                                }
                            }
                        }
                    }
                }

                None
            }
        })
    }
}
