package azkaban.viewer.hive;

import azkaban.webapp.servlet.LoginAbstractAzkabanServlet;
import azkaban.utils.Props;
import azkaban.webapp.servlet.Page;
import azkaban.server.session.Session;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;

public class HiveViewerServlet extends LoginAbstractAzkabanServlet {

  private final Props props;
  private final String viewerName;
  private final String viewerPath;

  public HiveViewerServlet(final Props props) {
    super(new ArrayList<>());
    this.props = props;
    this.viewerName = props.getString("viewer.name");
    this.viewerPath = props.getString("viewer.path");
  }

  @Override
  public void init(final ServletConfig config) throws ServletException {

    super.init(config);
  }

  @Override
  protected void handleGet(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    final boolean ajax = hasParam(req, "ajax");
    try {
      if (ajax) {
        System.out.println("do nothing");
      } else {
        final Page page =
        newPage(req, resp, session,
            "azkaban/viewer/hive/velocity/hive-browser.vm");
        page.render();
      }
    } catch (final Exception e) {
      throw new IllegalStateException("Error processing request: "
          + e.getMessage(), e);
    }
  }

  @Override
  protected void handlePost(final HttpServletRequest req, final HttpServletResponse resp,
      final Session session) throws ServletException, IOException {
    
  }

}