package org.mule.ant;

/**
 * Class representing a content type mapping.
 * <p/>
 * Content type mapping maps a content type
 *
 * @author Tadeusz Kozak
 *         Created at 10:30 10.05.12
 */
public class ContentTypeMapping
{

    /**
     * File extension part of the mapping.
     */
    private String extension;

    /**
     * Content-Type part of the mapping.
     */
    private String contentType;

    public ContentTypeMapping()
    {
    }

    public ContentTypeMapping(String extension, String contentType)
    {
        this.extension = extension;
        this.contentType = contentType;
    }

    public String getExtension()
    {
        return extension;
    }

    public void setExtension(String extension)
    {
        this.extension = extension;
    }

    public String getContentType()
    {
        return contentType;
    }

    public void setContentType(String contentType)
    {
        this.contentType = contentType;
    }
}
