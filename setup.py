#!/usr/bin/env python
from setuptools import setup, find_packages

if __name__ == "__main__":
    setup(name='zalert',
          version='0.1.0',
          description='ZTF alerts moving target checker',
          author="Michael S. P. Kelley",
          author_email="msk@astro.umd.edu",
          url="https://github.com/mkelley/zalert",
          packages=find_packages(),
          requires=['numpy', 'astropy', 'sbpy'],
          license='BSD',
          )
