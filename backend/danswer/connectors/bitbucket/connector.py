from typing import Any

from atlassian import Bitbucket
from atlassian.bitbucket import Cloud

from danswer.configs.app_configs import INDEX_BATCH_SIZE
from danswer.connectors.interfaces import LoadConnector
from danswer.connectors.interfaces import PollConnector
from danswer.utils.logger import setup_logger

logger = setup_logger()


def _convert_pr_to_document(pull_request):
    return None


class BitbucketConnector(LoadConnector, PollConnector):
    def __init__(
        self,
        project: str,
        repo: str,
        is_cloud: bool,
        state_filter: str = "ALL",
        workspace_base: str | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.project = project
        self.repo = repo
        self.is_cloud = is_cloud
        self.state_filter = state_filter
        self.workspace_base = workspace_base.rstrip("/") if workspace_base else None
        self.batch_size = batch_size

        self.bitbucket_client: Bitbucket | None = None

        logger.info(
            f"workspace_base: {self.workspace_base}, project: {self.project}, repo: {self.repo}"
        )

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        username = credentials["bitbucket_username"]
        access_token = credentials["bitbucket_access_token"]

        if self.is_cloud:
            self.bitbucket_client = Cloud(
                username=username, password=access_token, cloud=True
            )
        else:
            self.bitbucket_client = Bitbucket(
                url=self.workspace_base, username=username, password=access_token
            )
        return None

    def _fetch_from_bitbucket(self, bitbucket_client: Bitbucket):
        pull_requests = bitbucket_client.get_pull_requests(
            self.project, self.repo, state=self.state_filter, order="NEWEST"
        )

        for batch in pull_requests:
            for pr in batch:
                print("test")
