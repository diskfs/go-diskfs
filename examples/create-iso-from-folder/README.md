# Create ISO from Folder
This code provides a function CreateIsoFromFolder that creates an ISO image from a given folder. The ISO image will contain the contents of the source folder, including all subfolders and files.

## Usage
To use the `CreateIsoFromFolder` function, you need to provide the following arguments:

srcFolder: The path to the source folder that you want to include in the ISO image.
outputFileName: The path and file name of the resulting ISO image.
For example:

Copy code
CreateIsoFromFolder("my-folder", "my-image.iso")
This will create an ISO image called my-image.iso that contains the contents of the my-folder folder.

Implementation details
The CreateIsoFromFolder function first calculates the size of the source folder, and uses this size to create a new disk image. It then creates an ISO 9660 filesystem on the disk image, and copies the contents of the source folder to the filesystem. Finally, it finalizes the ISO filesystem, which is required in order to make the ISO image compliant with the ISO 9660 standard.

The code also provides a helper function called FolderSize that calculates the size of a given folder, including the sizes of all its subfolders and files.

Error handling
The code checks for errors at various points, and logs a fatal error if any error occurs. You may want to modify the error handling to fit your specific needs.