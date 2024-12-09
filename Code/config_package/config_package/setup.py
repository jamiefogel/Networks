from setuptools import setup, find_packages

setup(
    name="config_package",
    version="0.1.0",
    packages=find_packages(),
    description="A package for managing platform-specific configurations.",
    author="Your Name",
    author_email="your_email@example.com",
    url="https://your_project_url.example.com",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)