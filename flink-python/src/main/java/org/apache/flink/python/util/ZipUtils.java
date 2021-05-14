package org.apache.flink.python.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Utils used to extract zip files and try to restore the origin permissions of files.
 */
@Internal
public class ZipUtils {

	public static void extractZipFileWithPermissions(String zipFilePath, String targetPath) throws IOException {
		try (ZipFile zipFile = new ZipFile(zipFilePath)) {
			Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
			boolean isUnix = isUnix();

			while (entries.hasMoreElements()) {
				ZipArchiveEntry entry = entries.nextElement();
				File file;
				if (entry.isDirectory()) {
					file = new File(targetPath, entry.getName());
					if (!file.exists()) {
						if (!file.mkdirs()) {
							throw new IOException("Create dir: " + file.getAbsolutePath() + "failed!");
						}
					}
				} else {
					file = new File(targetPath, entry.getName());
					File parentDir = file.getParentFile();
					if (!parentDir.exists()) {
						if (!parentDir.mkdirs()) {
							throw new IOException("Create dir: " + file.getAbsolutePath() + "failed!");
						}
					}
					if (file.createNewFile()) {
						OutputStream output = new FileOutputStream(file);
						IOUtils.copyBytes(zipFile.getInputStream(entry), output);
					} else {
						throw new IOException("Create file: " + file.getAbsolutePath() + "failed!");
					}
				}
				if (isUnix) {
					int mode = entry.getUnixMode();
					if (mode != 0) {
						Path path = Paths.get(file.toURI());
						Set<PosixFilePermission> permissions = new HashSet<>();
						addIfBitSet(mode, 8, permissions, PosixFilePermission.OWNER_READ);
						addIfBitSet(mode, 7, permissions, PosixFilePermission.OWNER_WRITE);
						addIfBitSet(mode, 6, permissions, PosixFilePermission.OWNER_EXECUTE);
						addIfBitSet(mode, 5, permissions, PosixFilePermission.GROUP_READ);
						addIfBitSet(mode, 4, permissions, PosixFilePermission.GROUP_WRITE);
						addIfBitSet(mode, 3, permissions, PosixFilePermission.GROUP_EXECUTE);
						addIfBitSet(mode, 2, permissions, PosixFilePermission.OTHERS_READ);
						addIfBitSet(mode, 1, permissions, PosixFilePermission.OTHERS_WRITE);
						addIfBitSet(mode, 0, permissions, PosixFilePermission.OTHERS_EXECUTE);
						Files.setPosixFilePermissions(path, permissions);
					}
				}
			}
		}
	}

	private static boolean isUnix() {
		return OperatingSystem.isLinux() ||
			OperatingSystem.isMac() ||
			OperatingSystem.isFreeBSD() ||
			OperatingSystem.isSolaris();
	}

	private static void addIfBitSet(
		int mode,
		int pos,
		Set<PosixFilePermission> posixFilePermissions,
		PosixFilePermission posixFilePermissionToAdd) {
		if ((mode & 1L << pos) != 0L) {
			posixFilePermissions.add(posixFilePermissionToAdd);
		}
	}
}
