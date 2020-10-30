from setuptools import setup, find_packages

setup(
    name = 'synctool',
    version = '0.0.4',
    url = 'https://github.com/hail-is/synctool.git',
    author = 'Hail Team',
    author_email = 'hail@broadinstitute.org',
    description = 'synctool',
    packages = find_packages(),
    install_requires=[
        'boto3==1.16.4',
        'botocore==1.19.4',
        'google-cloud-storage==1.32.0'
    ],
    include_package_data=True
)
