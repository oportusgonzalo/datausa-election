import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    author="Walker Lambrecht",
    author_email="walker@datawheel.us",
    name="OpenFEC_Wrapper",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    description="Federal Election Comission API Wrapper",
    include_package_data=True,
    install_requires=[
        "pandas==0.24.2",
        "requests==2.22.0"
    ],
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    version="0.0.1",
    url="https://github.com/Datawheel/DataUSA-Election/OpenFEC_Wrapper",
)
