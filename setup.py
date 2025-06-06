from setuptools import setup

setup(
    name='Maga',
    version='3.0.0',
    description='A DHT crawler framework using asyncio.',
    long_description=open('README.rst', 'r').read(),
    author='whtsky',
    author_email='whtsky@gmail.com',
    url='https://github.com/whtsky/maga',
    license='BSDv3',
    platforms='any',
    zip_safe=False,
    include_package_data=True,
    py_modules=['maga'],
    install_requires=open("requirements.txt").readlines(),
    keywords=['dht', 'asyncio', 'crawler', 'bt', 'kad'],
    classifiers=[
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
