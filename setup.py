from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="chartinkbyvk",
    version="0.0",
    author="vk datta",
    description="A simple Interactive CLI to fetch union/intersect of multiple Chartink scans",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vkdatta/chartinkbyvk",
    packages=find_packages(),
    package_dir={"": "."},
    py_modules=[],
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.3.0",
        "beautifulsoup4>=4.9.0",
        "lxml>=4.6.0"
    ],
    entry_points={
        "console_scripts": [
            "chartink=script.chartink:main"
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
