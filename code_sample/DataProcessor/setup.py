from setuptools import setup, find_packages

setup(
    name="data-processor",
    version="1.0.0",
    description="A data processing library for handling large datasets",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.21.0",
        "python-dotenv>=0.19.0",
        "pytest>=6.2.5",
        "pytest-asyncio>=0.16.0",
    ],
    extras_require={
        "dev": [
            "black",
            "flake8",
            "mypy",
            "pytest-cov",
        ],
    },
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)