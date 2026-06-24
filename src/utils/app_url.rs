use urlencoding::encode;

pub(crate) fn app_project_url(
    app_url: &str,
    org_name: &str,
    project_name: &str,
    path_segments: &[&str],
) -> String {
    let mut url = format!(
        "{}/app/{}/p/{}",
        app_url.trim_end_matches('/'),
        encode(org_name),
        encode(project_name),
    );

    for segment in path_segments {
        if segment.is_empty() {
            continue;
        }
        url.push('/');
        url.push_str(&encode(segment));
    }

    url
}

pub(crate) fn app_project_url_with_encoded_path(
    app_url: &str,
    org_name: &str,
    project_name: &str,
    encoded_path: &str,
) -> String {
    let mut url = app_project_url(app_url, org_name, project_name, &[]);
    let encoded_path = encoded_path.trim_start_matches('/');
    if !encoded_path.is_empty() {
        url.push('/');
        url.push_str(encoded_path);
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_project_urls_encode_owned_segments_and_preserve_encoded_paths() {
        let cases = [
            (
                app_project_url(
                    "https://www.example.test/",
                    "test org",
                    "test project",
                    &["datasets", "dataset/name"],
                ),
                "https://www.example.test/app/test%20org/p/test%20project/datasets/dataset%2Fname",
            ),
            (
                app_project_url_with_encoded_path(
                    "https://www.example.test",
                    "test org",
                    "test project",
                    "tools?pr=function%2Fid",
                ),
                "https://www.example.test/app/test%20org/p/test%20project/tools?pr=function%2Fid",
            ),
        ];

        for (actual, expected) in cases {
            assert_eq!(actual, expected);
        }
    }
}
