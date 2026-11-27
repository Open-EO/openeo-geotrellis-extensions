#!/bin/sh
# Replaces the VITO-internal dnf repositories of the base image with the current AlmaLinux 9 and EPEL repositories,
# so the image can be built outside the VITO network (e.g. on GitHub Actions runners).
set -eu

cd /etc/yum.repos.d
rm -f alma_9.repo vito.repo epel.repo epel-testing.repo
[ -f epel.repo.rpmnew ] && mv epel.repo.rpmnew epel.repo

for repo in BaseOS AppStream CRB extras; do
  id=$(echo "$repo" | tr '[:upper:]' '[:lower:]')
  cat >> almalinux-current.repo <<EOF
[$id]
name=AlmaLinux 9 - $repo
baseurl=https://repo.almalinux.org/almalinux/9/$repo/\$basearch/os/
enabled=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-AlmaLinux-9

EOF
done

dnf clean all
