from setuptools import setup, find_packages

setup(
    name="chartinkbyvk",
    version="0.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "requests",
        "pandas",
        "beautifulsoup4"
    ],
    entry_points={
        "console_scripts": [
            "chartink=script.chartink:main"
        ]
    },
    python_requires=">=3.8"
)
