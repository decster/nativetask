DISTNAME=nativetask-0.1.0
DISTDIR=target/$DISTNAME
mkdir -p $DISTDIR
cp target/DISTNAME.jar $DISTDIR
cp -r target/native/target/usr/include $DISTDIR/
cp -r target/native/target/usr/lib $DISTDIR/
cd target ; tar czf ../prebuild/$DISTNAME.tar.gz $DISTNAME ; cd -
