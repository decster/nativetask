DISTNAME=nativetask-0.1.0
DISTDIR=target/$DISTNAME
mkdir -p $DISTDIR
cp target/hadoop-$DISTNAME.jar $DISTDIR
cp -r target/native/target/usr/local/include $DISTDIR/
cp -r target/native/target/usr/local/lib $DISTDIR/
cp INSTALL $DISTDIR/
cd target ; rm ../prebuild/$DISTNAME.tar.gz ; tar czf ../prebuild/$DISTNAME.tar.gz $DISTNAME ; cd -
