from setuptools import setup, find_packages

setup(
    name='qvarn-mr',
    version='0.1.7',
    license="LGPL",
    packages=find_packages('.'),
    include_package_data=True,
    install_requires=[
        'requests-futures',
        'qvarn-utils',
        'python-dateutil',
    ],
    entry_points={
        'console_scripts': [
            'qvarnmr-worker=qvarnmr.scripts.worker:main',
        ],
    },
)
