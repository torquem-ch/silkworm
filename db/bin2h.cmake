# https://gist.github.com/sivachandran/3a0de157dccef822a230
include(CMakeParseArguments)

# Function to wrap a given string into multiple lines at the given column position.
# Parameters:
#   VARIABLE    - The name of the CMake variable holding the string.
#   AT_COLUMN   - The column position at which string will be wrapped.
function(WRAP_STRING)
	set(oneValueArgs VARIABLE AT_COLUMN)
	cmake_parse_arguments(WRAP_STRING "${options}" "${oneValueArgs}" "" ${ARGN})

	string(LENGTH ${${WRAP_STRING_VARIABLE}} stringLength)
	math(EXPR offset "0")

	while(stringLength GREATER 0)

		if(stringLength GREATER ${WRAP_STRING_AT_COLUMN})
			math(EXPR length "${WRAP_STRING_AT_COLUMN}")
		else()
			math(EXPR length "${stringLength}")
		endif()

		string(SUBSTRING ${${WRAP_STRING_VARIABLE}} ${offset} ${length} line)
		set(lines "${lines}\n${line}")

		math(EXPR stringLength "${stringLength} - ${length}")
		math(EXPR offset "${offset} + ${length}")
	endwhile()

	set(${WRAP_STRING_VARIABLE} "${lines}" PARENT_SCOPE)
endfunction()

# Script to embed contents of a file as byte array in C/C++ header file(.h). The header file
# will contain a byte array and integer variable holding the size of the array.
# Parameters
#   SOURCE_FILE     - The path of source file whose contents will be embedded in the header file.
#   VARIABLE_NAME   - The name of the variable for the byte array. The string "_SIZE" will be append
#                     to this name and will be used a variable name for size variable.
#   HEADER_FILE     - The path of header file.
#   APPEND          - If specified appends to the header file instead of overwriting it
#   NULL_TERMINATE  - If specified a null byte(zero) will be append to the byte array. This will be
#                     useful if the source file is a text file and we want to use the file contents
#                     as string. But the size variable holds size of the byte array without this
#                     null byte.
set(options APPEND NULL_TERMINATE)
set(oneValueArgs SOURCE_FILE VARIABLE_NAME HEADER_FILE)
# cmake_parse_arguments(BIN2H "${options}" "${oneValueArgs}" "" ${ARGN})

# reads source file contents as hex string
file(READ ${BIN2H_SOURCE_FILE} hexString HEX)


# Minimize code (remove leading whitespace, and empty lines)
if (${BIN2H_REMOVE_LEADING_WHITESPACE})
# remove leading whitespaces
string(REGEX REPLACE "0d0a(09|20)+" "0d0a" hexString ${hexString})
string(REGEX REPLACE "0a0d(09|20)+" "0a0d" hexString ${hexString}) # MACOSX?
string(REGEX REPLACE "0a(09|20)+" "0a" hexString ${hexString})
endif()

if (${BIN2H_REMOVE_TRAILING_WHITESPACE})
# remove leading whitespaces
string(REGEX REPLACE "(09|20)+0d0a" "0d0a" hexString ${hexString})
string(REGEX REPLACE "(09|20)+0a0d" "0a0d" hexString ${hexString}) # MACOSX?
string(REGEX REPLACE "(09|20)+0a" "0a" hexString ${hexString})
endif()

if (${BIN2H_REMOVE_EMPTY_LINES})
# remove_empty_lines
string(REGEX REPLACE "(0d0a)(0d0a)+" "0d0a" hexString ${hexString})
string(REGEX REPLACE "(0a0d)(0a0d)+" "0a0d" hexString ${hexString}) # MACOSX?
string(REGEX REPLACE "0a(0a)+" "0a" hexString ${hexString})
endif()

if (${BIN2H_CONDENSE_WHITESPACE})
# condense all whitespace
string(REGEX REPLACE "(09|20)(09|20)+" "20" hexString ${hexString})
endif()


# wraps the hex string into multiple lines at column 32(i.e. 16 bytes per line)
wrap_string(VARIABLE hexString AT_COLUMN 32)

# adds '0x' prefix and comma suffix before and after every byte respectively
string(REGEX REPLACE "([0-9a-f][0-9a-f])" "0x\\1, " arrayValues ${hexString})
# removes trailing comma
string(REGEX REPLACE ", $" "" arrayValues ${arrayValues})

# declares byte array and the length variables
set(arrayDefinition_internal "static const char ${BIN2H_VARIABLE_NAME}_internal[] = {${arrayValues}\n};\n")

#file(WRITE ${BIN2H_HEADER_FILE} "${arrayDefinition}")

# add a pointer for extern use
set(arrayDefinitionExtern "const char* ${BIN2H_VARIABLE_NAME}(void){return &${BIN2H_VARIABLE_NAME}_internal[0];}\n")

# add a pointer for extern use
set(arraySizeofDefinitionExtern "size_t sizeof_${BIN2H_VARIABLE_NAME}(void){return sizeof(${BIN2H_VARIABLE_NAME}_internal);}\n")

set(file_content "/* Generated from ${BIN2H_SOURCE_FILE} */\n#include <stddef.h>\n${arrayDefinition_internal}${arrayDefinitionExtern}${arraySizeofDefinitionExtern}")

file(WRITE ${BIN2H_HEADER_FILE} "${file_content}")
