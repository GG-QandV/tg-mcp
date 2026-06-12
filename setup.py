#!/usr/bin/env python3
"""
45telega - Production-ready Telegram MCP Server
A Model Context Protocol server for Telegram integration
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Read requirements
requirements = []
with open("requirements.txt", "r") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and not line.startswith("-"):
            # Remove version specifiers for install_requires
            pkg = line.split("==")[0].split(">=")[0].split("<=")[0]
            requirements.append(line)

# Development requirements
dev_requirements = [
    "pytest>=7.4.4",
    "pytest-asyncio>=0.23.3",
    "pytest-cov>=4.1.0",
    "black>=24.1.1",
    "mypy>=1.8.0",
    "ruff>=0.1.14",
]

setup(
    name="45telega",
    version="1.0.0",
    author="Sergey Kostenchuk",
    author_email="support@45telega.io",
    description="Production-ready Telegram MCP Server with 45+ methods",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sergekostenchuk/45telega",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Communications :: Chat",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Framework :: AsyncIO",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
        "docker": ["docker>=7.0.0", "docker-compose>=1.29.2"],
    },
    entry_points={
        "console_scripts": [
            "45telega=mcp_telegram:app",
            "telega-mcp=mcp_telegram:app",
            "tgmcpd=mcp_telegram.daemon:main",
            "tg-mcp-proxy=mcp_telegram.proxy:run",
        ],
    },
    include_package_data=True,
    package_data={
        "mcp_telegram": [
            "config/*.yaml",
            "config/*.json",
            "templates/*.j2",
        ],
    },
    zip_safe=False,
    keywords=[
        "telegram",
        "mcp",
        "model-context-protocol",
        "telethon",
        "bot",
        "automation",
        "api",
    ],
    project_urls={
        "Bug Reports": "https://github.com/sergekostenchuk/45telega/issues",
        "Source": "https://github.com/sergekostenchuk/45telega",
        "Documentation": "https://github.com/sergekostenchuk/45telega/wiki",
    },
)