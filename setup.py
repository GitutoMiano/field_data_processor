from setuptools import setup, find_packages

setup(
    name='field_data_processor',
    version='0.1',
    packages=find_packages(exclude=['tests*']),
    license='MIT',
    description='EDSA example python package',
    long_description=open('README.md').read(),
    install_requires=['numpy'],
    url='https://github.com/GitutoMiano/data_ingestion',
    author='David G',
    author_email='gituto.david7@gmail.com'
)