:: @echo off
php -q replica2-win.php onlyphp
xcopy /Y rb.php "testing/cli/testcontainer"
cd testing/cli
php -q runtests.php
cd ../../
