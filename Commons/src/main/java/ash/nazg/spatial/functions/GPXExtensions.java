package ash.nazg.spatial.functions;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class GPXExtensions {
    public static void appendExt(Element extensions, String stat, Object value) {
        Element statEl = extensions.getOwnerDocument().createElement(stat);
        statEl.setTextContent(String.valueOf(value));
        extensions.appendChild(statEl);
    }

    public static Element getOrCreate(Document extensions) {
        Element segPropsEl = extensions.createElement("extensions");
        extensions.appendChild(segPropsEl);
        return segPropsEl;
    }
}
