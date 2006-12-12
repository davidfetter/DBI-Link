%define sname	dbi-link

Name:		postgresql-%{sname}
Version:	2.0.0
Release:	1%{?dist}
Summary:	Partial implementation of the SQL/MED portion of the SQL:2003 specification
Group:		Applications/Databases
License:	BSD
URL:		http://pgfoundry.org/projects/dbi-link/
Source0:	%{sname}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Requires:	postgresql-server >= 8.0, perl => 5.8.5, perl-YAML
BuildRequires:	perl => 5.8.5, postgresql-devel >= 8.0
BuildArch:	noarch

%description
If you've ever wanted to join PostgreSQL tables from other data you
can access via Perl's DBI, this is your project.

You can add speed and accuracy to your ETL processes by treating
any data source you can reach with DBI as a  PostgreSQL table.

%package test
Summary:	Test suite for dbi-link
Group:		Applications/Databases
Requires:	%{name}

%description test
The test suite of dbi-link

%prep
%setup -q -n %{sname}-%{version}

%build

%install
rm -rf %{buildroot}
install -d %{buildroot}/%{_datadir}/%{name}/
install -m 644 *.sql %{buildroot}/%{_datadir}/%{name}/

# test
install -d %{buildroot}/%{_datadir}/%{name}/test
cp -rp test/* %{buildroot}/%{_datadir}/%{name}/test

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%doc copyright.txt  IMPLEMENTATION.txt README.txt TODO.txt ROADMAP.txt
%{_datadir}/%{name}/*.sql

%files test
%defattr(-,root,root,-)
%doc README.txt
%{_datadir}/%{name}/test/*

%changelog
* Tue Dec 12 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 2.0.0-1
- Update to 2.0.0

* Fri Jul 21 2006 - Devrim GUNDUZ <devrim@commandprompt.com> 1.0.0-2
- 1.1.0
- Fixed rpmlint errors

* Thu Dec 29 2005 - Devrim GUNDUZ <devrim@commandprompt.com> 1.0.0
- Initial version
