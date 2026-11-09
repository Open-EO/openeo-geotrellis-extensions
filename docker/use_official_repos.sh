#!/bin/sh
# Replaces the VITO-internal dnf repositories of the base image with the official AlmaLinux vault and EPEL repositories,
# so the image can be built outside the VITO network (e.g. on GitHub Actions runners).
set -eu

cd /etc/yum.repos.d
rm -f alma_9.repo vito.repo epel.repo epel-testing.repo
[ -f epel.repo.rpmnew ] && mv epel.repo.rpmnew epel.repo

for repo in BaseOS AppStream CRB extras; do
  id=$(echo "$repo" | tr '[:upper:]' '[:lower:]')
  cat >> almalinux-vault.repo <<EOF
[$id]
name=AlmaLinux \$releasever_major.\$releasever_minor - $repo (vault)
baseurl=https://vault.almalinux.org/\$releasever_major.\$releasever_minor/$repo/\$basearch/os/
enabled=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-AlmaLinux-9

EOF
done

dnf clean all
