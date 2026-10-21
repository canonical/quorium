import datetime
import os

project = "Quorium"
author = "Canonical Ltd."
copyright = f"{datetime.date.today().year}"
release = os.environ.get("DOCS_VERSION", "latest")
version = release

html_theme = "ulwazi"
html_title = f"{project} documentation"
html_baseurl = os.environ.get("DOCS_BASE_URL", "/")

html_context = {
    "product_page": "github.com/canonical/quorium",
    "github_url": "https://github.com/canonical/quorium",
    "repo_branch": "main",
    "repo_folder": "/documentation/",
    "default_source_extension": ".md",
    "feedback": True,
    "discourse": "",
    "mattermost": "",
    "matrix": "",
    "logo_title": "Canonical",
    "logo_link_URL": "https://canonical.com",
    "logo_img_URL": "https://assets.ubuntu.com/v1/82818827-CoF_white.svg",
    "author": author,
    "footer": {"product": True, "license": True},
    "license": {
        "name": "MIT OR Apache-2.0",
        "url": "https://github.com/canonical/quorium#license",
    },
}

extensions = [
    "myst_parser",
    "sphinx_design",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "sphinx_sitemap",
    "sphinxext.opengraph",
]

source_suffix = {".md": "markdown"}
root_doc = "index"
exclude_patterns = [".venv", "_build", "Thumbs.db", ".DS_Store"]

myst_enable_extensions = ["colon_fence", "deflist", "attrs_inline"]
myst_heading_anchors = 3
myst_fence_as_directive = ["mermaid"]

copybutton_exclude = ".linenos, .gp, .go"
copybutton_prompt_text = "$ "

sitemap_url_scheme = "{link}"
sitemap_excludes = ["404/", "genindex/", "search/"]

ogp_site_url = html_baseurl
ogp_site_name = html_title
ogp_image = "https://assets.ubuntu.com/v1/cc828679-docs_illustration.svg"

notfound_urls_prefix = os.environ.get("DOCS_URL_PREFIX", "/")

linkcheck_ignore = [r"http://127\.0\.0\.1.*", r"http://localhost.*", r"https://crates\.io/.*"]
linkcheck_anchors_ignore_for_url = [r"https://github\.com/.*"]
linkcheck_retries = 3
linkcheck_timeout = 15
