svn update
rm -f module.zip
rm -f */*.pyc
zip module.zip -r config common consumer db hbase job service util
