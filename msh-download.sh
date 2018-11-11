#!/usr/bin/env bash

curl -s 'http://www.sharett.org.il/cgi-webaxy/sal/sal.pl?lang=he&ID=880900_sharett_new&dbid=pics&Articles&act=search2&query=offset<D>0<D>sort<D>f16<D>sort2<D>f6' > data/msh/source-html/page1.html &&\
curl -s 'http://www.sharett.org.il/cgi-webaxy/sal/sal.pl?lang=he&ID=880900_sharett_new&dbid=pics&Articles&act=search2&query=offset<D>300<D>sort<D>f16<D>sort2<D>f6' > data/msh/source-html/page2.html &&\
curl -s 'http://www.sharett.org.il/cgi-webaxy/sal/sal.pl?lang=he&ID=880900_sharett_new&dbid=pics&Articles&act=search2&query=offset<D>600<D>sort<D>f16<D>sort2<D>f6' > data/msh/source-html/page3.html &&\
curl -s 'http://www.sharett.org.il/cgi-webaxy/sal/sal.pl?lang=he&ID=880900_sharett_new&dbid=pics&Articles&act=search2&query=offset<D>900<D>sort<D>f16<D>sort2<D>f6' > data/msh/source-html/page4.html &&\
curl -s 'http://www.sharett.org.il/cgi-webaxy/sal/sal.pl?lang=he&ID=880900_sharett_new&dbid=pics&Articles&act=search2&query=offset<D>1200<D>sort<D>f16<D>sort2<D>f6' > data/msh/source-html/page5.html
[ "$?" != "0" ] && echo Download Failed && exit 1
echo Great Success!
exit 0
