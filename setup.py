from setuptools import setup, find_packages
import os


def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()


__version__ = "0.0.4"

setup(name='easymirror',
      version=__version__,
      keywords='Finance easy futures makeup',
      description='Rsync financial datasource.',
      long_description=read("README.md"),
      license='GPL',

      url='https://github.com/lamter/easymirror',
      author='lamter',
      author_email='lamter.fu@gmail.com',

      packages=find_packages(),
      include_package_data=True,
      install_requires=read("requirements.txt").splitlines(),
      classifiers=[
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
      ],
      )
