from setuptools import setup, find_packages
import NexR_dqc

VERSION = NexR_dqc.__version__

setup(
    name = 'NexR_dqc',
    version = VERSION,
    packages = find_packages(),
    description = "Data EDA & QC Package with Python",
    long_description = open('README.md').read(), 
    long_description_content_type = 'text/markdown',
    url = "https://github.com/Leejung8763/NexR_dqc",
    download_url = f"https://github.com/Leejung8763/NexR_dqc/archive/refs/tags/{VERSION}.tar.gz",
    author = "Lee Jung",
    author_email = "leejung8763@naver.com",
    keywords = ["pypi"],
    python_requires = ">=3",
    install_requires = ["numpy", "pandas==1.2.4", "pyarrow", "openpyxl", "xlsxwriter"],
    license = "MIT",
    classifiers = [
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ]
)