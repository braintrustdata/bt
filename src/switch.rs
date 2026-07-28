use anyhow::{bail, Context, Result};
use clap::Args;

use crate::args::{ArgValueSource, BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL};
use crate::auth::{self, login, AvailableInstance, AvailableOrg};
use crate::config;
use crate::http::ApiClient;
use crate::ui::{can_prompt, print_command_status, select_or_create_project, CommandStatus};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt switch
  bt switch test-project
  bt switch test-org/test-project
")]
pub struct SwitchArgs {
    #[command(flatten)]
    scope: config::ScopeArgs,

    /// Target: project name or org/project
    #[arg(value_name = "TARGET")]
    target: Option<String>,
}

impl SwitchArgs {
    fn resolve_target(&self, base: &BaseArgs) -> (Option<String>, Option<String>) {
        let (pos_org, pos_project) = match &self.target {
            None => (None, None),
            Some(target) if target.contains('/') => {
                let parts: Vec<&str> = target.splitn(2, '/').collect();
                let org = (!parts[0].trim().is_empty()).then(|| parts[0].trim().to_string());
                let project = (!parts[1].trim().is_empty()).then(|| parts[1].trim().to_string());
                (org, project)
            }
            Some(target) => (None, Some(target.clone())),
        };

        (
            base.org_name
                .as_ref()
                .filter(|_| {
                    matches!(
                        base.org_name_source,
                        Some(ArgValueSource::CommandLine | ArgValueSource::EnvVariable)
                    )
                })
                .cloned()
                .or(pos_org),
            base.project.clone().or(pos_project),
        )
    }
}

fn find_org<'a>(orgs: &'a [AvailableOrg], identifier: &str) -> Option<&'a AvailableOrg> {
    orgs.iter()
        .find(|org| org.id == identifier || org.name == identifier)
        .or_else(|| {
            let lowered = identifier.to_ascii_lowercase();
            orgs.iter()
                .find(|org| org.name.to_ascii_lowercase() == lowered)
        })
}

fn select_instance(
    instances: &[AvailableInstance],
    current_app_url: Option<&str>,
) -> Result<AvailableInstance> {
    match instances {
        [] => bail!("no saved auth logins found; run `bt auth login` to create one"),
        [instance] => Ok(instance.clone()),
        _ if can_prompt() => {
            let labels = instances
                .iter()
                .map(|instance| instance.app_url.as_str())
                .collect::<Vec<_>>();
            let default = current_app_url
                .and_then(|current| {
                    instances
                        .iter()
                        .position(|instance| config::urls_equal(&instance.app_url, current))
                })
                .unwrap_or(0);
            let idx = crate::ui::fuzzy_select("Select Braintrust instance", &labels, default)?;
            Ok(instances[idx].clone())
        }
        _ => bail!(
            "multiple Braintrust instances are available; pass --app-url <URL> or rerun interactively"
        ),
    }
}

fn select_org(
    orgs: &[AvailableOrg],
    requested: Option<&str>,
    current: Option<&str>,
) -> Result<AvailableOrg> {
    if let Some(requested) = requested {
        return find_org(orgs, requested)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("organization '{requested}' is not available"));
    }
    match orgs {
        [] => bail!("no organizations are available for the selected Braintrust instance"),
        [org] => Ok(org.clone()),
        _ if can_prompt() => {
            let labels = orgs.iter().map(|org| org.name.as_str()).collect::<Vec<_>>();
            let default = current
                .and_then(|current| {
                    orgs.iter()
                        .position(|org| org.id == current || org.name == current)
                })
                .unwrap_or(0);
            let idx = crate::ui::fuzzy_select("Select organization", &labels, default)?;
            Ok(orgs[idx].clone())
        }
        _ => bail!("organization selection requires an interactive terminal; pass --org <ORG>"),
    }
}

pub(crate) async fn select_context(
    base: &BaseArgs,
    requested_org: Option<&str>,
    requested_project: Option<&str>,
    current_cfg: &config::Config,
    project_prompt: Option<&str>,
) -> Result<(
    AvailableInstance,
    AvailableOrg,
    crate::projects::api::Project,
)> {
    let instances = auth::available_instances(base)?;
    let instance = select_instance(&instances, current_cfg.app_url.as_deref())?;
    let orgs = auth::available_orgs_for_instance(base, &instance.app_url).await?;
    let same_current_instance = config::urls_equal(
        current_cfg.app_url.as_deref().unwrap_or(DEFAULT_APP_URL),
        &instance.app_url,
    );
    let current_org = same_current_instance
        .then(|| current_cfg.org_id.as_deref().or(current_cfg.org.as_deref()))
        .flatten();
    let org = select_org(&orgs, requested_org, current_org)?;

    let explicit_api_url = matches!(
        base.api_url_source,
        Some(ArgValueSource::CommandLine | ArgValueSource::EnvVariable)
    )
    .then(|| base.api_url.clone())
    .flatten();
    let current_api_url = same_current_instance
        .then(|| current_cfg.api_url.clone())
        .flatten();
    let api_url = explicit_api_url
        .or_else(|| org.api_url.clone())
        .or(current_api_url)
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    let mut login_base = base.clone();
    login_base.app_url = Some(instance.app_url.clone());
    login_base.api_url = Some(api_url);
    login_base.org_name = Some(org.name.clone());
    login_base.org_id = Some(org.id.clone());
    login_base.project = None;
    login_base.project_source = None;

    let ctx = login(&login_base).await?;
    let client = ApiClient::new(&ctx)?;
    let current_project = (same_current_instance
        && current_cfg.org_id.as_deref() == Some(org.id.as_str()))
    .then_some(current_cfg.project.as_deref())
    .flatten();
    let project =
        select_or_create_project(&client, requested_project, current_project, project_prompt)
            .await?;

    Ok((instance, org, project))
}

pub async fn run(base: BaseArgs, args: SwitchArgs) -> Result<()> {
    args.scope.preflight(can_prompt())?;
    let current_cfg = if args.scope.global {
        config::load_global().unwrap_or_default()
    } else {
        config::load().unwrap_or_default()
    };
    let (requested_org, requested_project) = args.resolve_target(&base);
    let (instance, org, project) = select_context(
        &base,
        requested_org.as_deref(),
        requested_project.as_deref(),
        &current_cfg,
        None,
    )
    .await?;
    let api_url = if matches!(
        base.api_url_source,
        Some(ArgValueSource::CommandLine | ArgValueSource::EnvVariable)
    ) {
        base.api_url.clone()
    } else {
        org.api_url.clone().or_else(|| {
            config::urls_equal(
                current_cfg.app_url.as_deref().unwrap_or(DEFAULT_APP_URL),
                &instance.app_url,
            )
            .then(|| current_cfg.api_url.clone())
            .flatten()
        })
    }
    .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    let (path, scope) = args.scope.resolve(can_prompt(), "Save to")?;
    let mut cfg = config::load_file(&path);
    cfg.set_context(
        (org.name.as_str(), org.id.as_str()),
        Some((project.name.as_str(), project.id.as_str())),
        &instance.app_url,
        &api_url,
    );
    config::save_file(&path, &cfg)
        .with_context(|| format!("Could not save config to {}", path.display()))?;

    if base.json {
        let payload = serde_json::json!({
            "org": org.name,
            "org_id": org.id,
            "project": project.name,
            "project_id": project.id,
            "app_url": instance.app_url,
            "api_url": api_url,
            "scope": scope,
            "path": path.display().to_string(),
        });
        println!("{}", serde_json::to_string(&payload)?);
        return Ok(());
    }

    print_command_status(
        CommandStatus::Success,
        &format!("Switched to {}/{}", org.name, project.name),
    );
    if base.verbose {
        eprintln!("Wrote to {}", path.display());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_target_combines_positionals_and_flags() {
        let args = SwitchArgs {
            scope: config::ScopeArgs::default(),
            target: Some("test-org/test-project".to_string()),
        };
        let base = BaseArgs::default();
        let actual = args.resolve_target(&base);
        assert_eq!(actual.0.as_deref(), Some("test-org"));
        assert_eq!(actual.1.as_deref(), Some("test-project"));
    }

    #[test]
    fn find_org_matches_name_id_and_case() {
        let orgs = vec![AvailableOrg {
            id: "org_test".to_string(),
            name: "test-org".to_string(),
            api_url: None,
        }];
        assert!(find_org(&orgs, "org_test").is_some());
        assert!(find_org(&orgs, "TEST-ORG").is_some());
    }
}
