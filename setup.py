from setuptools import setup, find_packages

setup(
    name='mystore',
    version='0.2',
    author='pyvlad',
    author_email='pyvlad.it@ya.ru',
    python_requires=">=3.5.0",
    packages=find_packages(),
    scripts=[],
    description=("""My customizable key:value store."""),
    install_requires=[]
)
