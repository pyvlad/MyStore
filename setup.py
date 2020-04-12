import os
from setuptools import setup, find_packages, Command


class CleanCommand(Command):
    """
    Custom clean command to tidy up the project root.
    https://stackoverflow.com/questions/3779915/why-does-python-setup-py-sdist-create-unwanted-project-egg-info-in-project-r
    """
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


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
    },
    cmdclass={
        'clean': CleanCommand,
    }
)
# optional dependencies: plyvel, sqlalchemy 
