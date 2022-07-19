use azure_storage::clients::StorageClient;
use azure_storage_blobs::prelude::AsContainerClient;
use futures::StreamExt;
use lifec::{
    plugins::{Plugin, ThunkContext},
    Component, DenseVecStorage,
};

/// Plugin provides access to azure container
///
#[derive(Clone, Default, Component)]
#[storage(DenseVecStorage)]
pub struct Container;

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
                        let account = std::env::var("STORAGE_ACCOUNT")
                            .expect("Set env variable STORAGE_ACCOUNT first!");
                        let access_key = std::env::var("STORAGE_ACCESS_KEY")
                            .expect("Set env variable STORAGE_ACCESS_KEY first!");

                        let storage_client = StorageClient::new_access_key(&account, &access_key);
                        let blob_client = storage_client
                            .container_client(&container_name)
                            .blob_client(&blob_name);

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

                None
            }
        })
    }
}
