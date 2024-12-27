package org.example;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * SpringBoot控制类
 */
@Controller
public class MyController {
    /**
     * 查询用户列表
     * @throws Exception
     */
    @RequestMapping("/index")
    public String index(HttpServletRequest request,
                        HttpServletResponse response) throws Exception {
        System.out.println("控制类测试数据...");
        return "index";
    }
}
