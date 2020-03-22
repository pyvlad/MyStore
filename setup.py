from setuptools import setup, find_packages

setup(
    name='pyvlad-mystore',
    version='0.1.0',
    author='pyvlad',
    author_email='pyvlad.it@ya.ru',
    python_requires=">=3.5.0",
    packages=find_packages(),
    scripts=[],
    description=("""My customizable key:value store."""),
    install_requires=[],
    extras_require={
        "storage": ["sqlalchemy"],
        "leveldb": ["plyvel"]
    }
)
