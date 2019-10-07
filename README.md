GSM LocationProvider backend for µg UnifiedNlp
==============================================

MicroG has a unified network location provider that accepts backends for
the actual location lookup.
This project implements a cell-tower based lookup for your current location.
It ships a binary cell database extracted from opencellid and thus licensed
under the CC-BY-SA license. Review the following content for details:
- res/assets/towers.bcs.xz (the data, xz compressed)
- https://creativecommons.org/licenses/by-sa/4.0/ (the license)
- https://wiki.opencellid.org/wiki/Licensing%3A (the license statement of the source)

This "NetworkLocationProvider" works without network connectivity and will
never post your data anywhere. You are thus encouraged to help opencellids
to gather more cells in order to improve this project.

This software includes "XZ for Java". The files were put into public domain
and licensed as "do whatever you want with these files". You are thus free
to take everything under src/org/tukaani and treat it as public domain.

This software is licensed as "Apache License, Version 2.0" unless noted
otherwise.
