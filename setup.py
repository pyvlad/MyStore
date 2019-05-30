from setuptools import setup, find_packages

setup(
    name='dbmdb',
    version='0.1',
    author='pyvlad',
    author_email='pyvlad.it@ya.ru',
    python_requires=">=3.5.0",
    packages=find_packages(),
    scripts=[],
    description=("""A wrapper over basic storing functionality in dbm files."""),
    install_requires=[]
)
