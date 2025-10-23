# setup.py（放在项目根目录）
from setuptools import setup, find_packages

setup(
    name="crazy-cloud-instance-handler",
    version="1.0.0",
    description="A tool to handle cloud instance data from Alibaba Cloud, AWS, etc.",
    author="Crazy",
    author_email="raindream233@foxmail.com",
    packages=find_packages(),                # 自动发现：cloudinstancedatahandler, sender, test 等
    package_data={
        'cloudinstancehandler': ['py.typed'],  # 如果是库，可选
    },
    include_package_data=False,
    python_requires=">=3.7",
    install_requires=[
        "pandas",
        "openpyxl",
        "requests",
    ],
    # entry_points={
        # 可选：命令行工具
        # 'console_scripts': [
        #     'cloud-data-cli=cloudinstancedatahandler.cli:main',
        # ],
    # },
)

# 请 cd 到项目根目录，然后运行以下命令：
# pip install -e .