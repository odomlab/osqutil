#!/usr/bin/perl
#
#
#
#
use strict;
use warnings;

sub isFileInIRODs {
	my $file = shift;

	my $res = `ils $file`;
	chomp($res);
	$res =~ s/^\s*//;
	print "isFileInIRODs result \"$res\"\n";
	if($res eq $file) {
		return 1;	
	}
	else {
		return 0;
        }
}

sub filePathInIRODs {
	my $file = shift;

	my $run = substr($file,0,index($file,"_"));
	my $filefull = "/seq/$run/$file";
	return $filefull; 
}

sub getFileInIRODs {
	my $file = shift;
	my $destination = shift;
	
	my $filefull = &filePathInIRODs($file);
	print STDERR "File: $filefull\n";
	if(&isFileInIRODs($filefull)) {
   	        my $outfile = $file;
	        $outfile =~ s/\.cram \z/\.bam/ixms;
		if(!-f "$destination/$outfile") {
			my $cmd = "iget $filefull - | samtools view -b -F 0xF00 - > $destination/$outfile";
			print STDERR "$cmd\n";
			system($cmd);
		}
		else {
			print STDERR "$outfile exists in destination. Skipping!\n";
		}
		if(-f "$destination/$outfile") {
			return 1;
		}
		else {
			print STDERR "Coping file $file to $destination failed!\n";
			return 0;
		}
	}
	else {
		print STDERR "File $file not found!\n";	
		return 0;
	}
}

sub splitArgumentToLanes {
	my $str = shift;

	my $run=substr($str,0,index($str,"_"));
	my @lanes=split(/\,/,$str);
	foreach my $lane(@lanes) {
		if($lane =~ /^\d$/) {
			$lane=$run."_".$lane;
		}
	}
	return @lanes;
}

sub getMetaDataForFileInIRODs {
	my $file = shift;
	my $destination = shift;

	my $filefull = &filePathInIRODs($file);
	my $metafile = "$destination/$file.meta";
	$metafile =~ s/\.cram.meta \z/\.bam.meta/ixms;
	my $cmd = "imeta ls -d $filefull > $metafile";
	system($cmd);
	if(-f $metafile) {
		return 1;
	}
	else {
		print STDERR "Failed to get metadata for $file";
		return 0;
	}
}

sub listFilesInIRODs {
	# filepattern is either full file name or a filename prefix which has to contain at least the run number
	# E.g. "7384" or "7384_1" etc.
	my $filepattern = shift;
	my @files = ();

	my $run = substr($filepattern,0,index($filepattern,"_"));
	my @tmp = `ils /seq/$run/ | grep $filepattern`;
	foreach my $file(@tmp) {
		chomp($file);
		$file =~ s/^\s*//;
		if($file =~ /^\d+_\d/) {
			push(@files,$file);
		}
	}
	return @files;
}

my $usage="Usage: getDataFromIRODs.pl <filepreffix>\n\nExample: getDataFromIRODs.pl 7384_1,2,3\n\n";
my $repository = "/nfs/teams/team168/repository/bam/";
if(@ARGV != 1) {
	print STDERR $usage;
	exit 1;	
}
my $argument = $ARGV[0];
my @lanes = &splitArgumentToLanes($argument);
foreach my $lane (@lanes) {
	print "Processing lane \"$lane\" ...\n";
	my @files = &listFilesInIRODs($lane);

        FILE:
	foreach my $file(@files) {

	        # We're only interested in .bam/.cram and associated metadata.
   	        next FILE unless $file =~ /\.(bam|cram)$/i;
		print "Fetching $file ...\n";
		&getFileInIRODs($file,$repository);
		print "Fetching metadata for $file ...\n";
		&getMetaDataForFileInIRODs($file,$repository);
	}
}
