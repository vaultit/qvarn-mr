from setuptools import setup, find_packages

setup(
    name='qvarn-mr',
    version='0.1',
    packages=find_packages('.'),
    include_package_data=True,
    install_requires=[
        'requests-futures',
        'qvarn-utils',
        'python-dateutil',

        # Test dependencies
        'pytest',
        'pytest-mock',
        'pytest-cov',
        'requests-mock',
        'webtest',

        # Qvarn dependencies
        # https://github.com/ProgrammersOfVilnius/qvarn/releases
        'PyJWT',
        'bottle',
        'psycopg2',
        'pyyaml',
        'uwsgidecorators',
        'qvarn',
    ],
    entry_points={
        'console_scripts': [
            'qvarnmr-resync=qvarnmr.scripts.resync:main',
            'qvarnmr-worker=qvarnmr.scripts.worker:main',
        ],
    },
)
