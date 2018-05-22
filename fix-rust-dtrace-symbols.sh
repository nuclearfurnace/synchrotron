#!/bin/bash
sed -i -- 's/\$LT\$/\&lt;/g' profile.svg
sed -i -- 's/\$GT\$/\&gt;/g' profile.svg
sed -i -- 's/\$RF\$/\&amp;/g' profile.svg
sed -i -- 's/\$u20\$/ /g' profile.svg
sed -i -- 's/\$u27\$/'"'"'/g' profile.svg
sed -i -- 's/\$u5b\$/[/g' profile.svg
sed -i -- 's/\$u5d\$/]/g' profile.svg
sed -i -- 's/\$u7b\$/{/g' profile.svg
sed -i -- 's/\$u7d\$/}/g' profile.svg
sed -i -- 's/\$LP\$/(/g' profile.svg
sed -i -- 's/\$RP\$/)/g' profile.svg
sed -i -- 's/\$C\$/,/g' profile.svg
sed -i -- 's/\.\./::/g' profile.svg
