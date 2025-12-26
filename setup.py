from setuptools import setup, find_packages

setup(
    name="chartinkbyvk",
    version="0.0",
    py_modules=["chartink"],
    install_requires=[
        "requests",
        "pandas",
        "beautifulsoup4"
    ],
    entry_points={
        "console_scripts": [
            "chartink=chartinkbyvk:main"
        ]
    },
    python_requires=">=3.8"
)
