#!/usr/bin/env python3

from setuptools import setup


def parse_requirements():
    requirements = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            requirements.append(line)

    return requirements


setup(
    name='swh.loader.git',
    description='Software Heritage loader git utilities',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/DLDG/',
    packages=['swh.loader.git', 'swh.loader.git.tests'],
    scripts=['bin/swh-backend', 'bin/swh-db-manager', 'bin/swh-loader-git'],
    install_requires=parse_requirements(),
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
