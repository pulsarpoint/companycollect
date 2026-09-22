// noVNC 1.7 exports RFB at the package root; the published types still use 1.6's path.
declare module "@novnc/novnc" {
  import RFB from "@novnc/novnc/lib/rfb";
  export default RFB;
}
