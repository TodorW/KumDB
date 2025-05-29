from setuptools import setup, find_packages

setup(
    name='KumDB',
    version='3.0.0',
    author='Vuk Todorovic',
    author_email='vuk.todorovic@protonmail.com',
    description='A custom file-based DB system with JSON, CSV, msgpack support',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/TodorW/KumDB',
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=[
    'msgpack>=1.0.0',
    'typing_extensions>=4.0.0',
    'cryptography>=3.4',
    'dataclasses; python_version < "3.7"',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',  
        'Operating System :: OS Independent',
    ],
)
