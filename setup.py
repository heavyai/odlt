from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='odlt',
      version='0.1',
      description='Automation for importing and exporting packages of datasets, dashboards, and views in OmniSci Core / Immerse',
      url='http://github.com/omnisci/odlt',
      author='JP Harvey',
      author_email='jpharvey@omnisci.com',
      license='Apache',
      packages=['odlt'],
      install_requires=[
          'pymapd',
          'pytest',
          'markdown',
          'boto3',
      ],
      zip_safe=False)
